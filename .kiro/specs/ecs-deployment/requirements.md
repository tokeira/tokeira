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

## Prerequisite: IaC Progress Reporting and CLI UX

Progress callbacks on `ProvisionContext`, the `tkr` CLI output module (`ActionTuiHandle`, `OutputFormat`, `ProgressEvent`), and the JSON event schema are owned by the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) spec — see its Requirement 5 for the canonical callback surface (`set_apply_progress`, `set_complete_progress`, `set_failed_progress`, `set_wait_progress`, `set_note_progress`) and Requirement 5.14 for `--json` threading.

**Confirmation prompts** for mutating commands (`tkr infra apply|destroy`, `tkr deploy apply`, `tkr scale up|down`, `tkr debug logs enable|disable`, `tkr port-forward`, `tkr exec`) follow the rules in [`tkr-cli`](../tkr-cli/requirements.md): interactive confirmation by default, `--yes` bypass for automation, refuse to proceed when stdout is non-TTY and `--yes` is not provided.

This spec consumes those surfaces. It does not redefine them.

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
4. THE tagging SHALL apply to: VPC resources, security groups, VPC endpoints, ALB, ECS cluster, capacity providers, ASGs, launch templates, IAM roles, S3 buckets, DSQL cluster, Cloud Map namespace, ECS services/task definitions, and CloudWatch log groups (including the ecs-exec audit log group from Req 3.4).

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

1. THE ECS_Platform SHALL provision an internal ALB in the private subnets. THE ALB SHALL be scheme `internal` — no public IP, no internet gateway dependency.
2. THE Internal_ALB SHALL have two target groups: `tokeira-edge-api` and `tokeira-edge-poll`. Both target groups SHALL use `ProtocolVersion: GRPC` because edge services speak gRPC over HTTP/2.
3. THE Internal_ALB SHALL have a single listener on port 443 using HTTP/2. TLS SHALL be terminated at the ALB using an ACM certificate issued for the private DNS zone; operators who do not want TLS SHALL be able to configure `alb.listener_protocol = "http2"` to use cleartext HTTP/2 on port 80 (private-only deployment, no external exposure).
4. THE Internal_ALB SHALL support split private DNS names: `edge-api.<private-zone>` and `edge-poll.<private-zone>`.
5. THE target group health checks SHALL use the gRPC health check protocol (status code 0 for healthy) rather than HTTP 200.
6. THE Internal_ALB SHALL be provisioned as IaC resources implementing the `Resource` trait.
7. THE ALB SHALL NOT register the Grafana, Mimir, or Loki services. Observability services are reached via `tkr port-forward` (Requirement 9.6).
8. IF the operator configures `alb.listener_protocol = "https"`, THEN `alb.certificate_arn` SHALL be a required config field referencing an ACM certificate issued for the private DNS zone. THE ECS_Platform SHALL NOT attempt to request or renew ACM certificates itself — certificate provisioning is operator-managed (Private CA, DNS validation of an externally-issued cert, etc.). IF `alb.listener_protocol = "http2"`, THEN `alb.certificate_arn` SHALL be omitted and the listener SHALL use cleartext HTTP/2 on port 80.

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

**User Story:** As a Tokeira operator, I want one EC2-backed capacity provider per service plane with dedicated Auto Scaling groups, so that each plane (application, observability, control) scales independently and noisy neighbours cannot starve critical services.

#### Acceptance Criteria

1. THE ECS_Platform SHALL provision eight Capacity_Providers: `cp-edge-api`, `cp-edge-poll`, `cp-runtime`, `cp-projection`, `cp-control`, `cp-mimir`, `cp-loki`, `cp-grafana`.
2. WHEN a Capacity_Provider is provisioned, THE ECS_Platform SHALL create a backing Auto_Scaling_Group with configurable instance type, min/max/desired capacity, and subnet placement.
3. THE `cp-runtime` Auto_Scaling_Group SHALL have instance scale-in protection enabled by default on all instances.
4. THE Capacity_Providers and Auto_Scaling_Groups SHALL be provisioned as IaC resources implementing the `Resource` trait.
5. THE ECS_Cluster SHALL be associated with all eight Capacity_Providers via a cluster capacity provider strategy.
6. `cp-mimir`, `cp-loki`, and `cp-grafana` SHALL each run exactly one instance at default (min=1, max=1, desired=1). Max=1 guarantees `tkr port-forward` targets the unique instance hosting the service. A second instance during rolling replacement briefly violates this — operators accept the short window; `tkr port-forward` retries on the new instance if the incumbent has already been drained.
7. THE `cp-runtime` Auto_Scaling_Group SHALL set `new_instances_protected_from_scale_in = true` (Req 3.2.3). THE `cp-runtime` Capacity_Provider SHALL set `managed_termination_protection = "DISABLED"` — despite the per-instance protection. DAEMON services do not satisfy ECS's precondition for capacity-provider-managed termination protection (at least one REPLICA task on the host); the safety guarantee for runtime scale-in comes from Loop C (nominate → drain → clear per-instance protection → `TerminateInstanceInAutoScalingGroup` with `ShouldDecrementDesiredCapacity=true`), not from the capacity-provider-level toggle. All other capacity providers SHALL also set `managed_termination_protection = "DISABLED"` so they can scale to zero when idle.
8. THE `cp-control` Auto_Scaling_Group SHALL have `max_capacity >= 3` to leave headroom for rolling deployments. At `deployment_maximum_percent = 200%`, the steady-state 2×controller + 2×autoscaler + sidecars briefly doubles during a rolling update and exceeds a single `c8g.large`'s budget. THE control-plane ECS services SHALL set `deployment_maximum_percent = 200` and `deployment_minimum_healthy_percent = 50` so rolling updates do not sacrifice HA.

### Requirement 3.3: Auto Scaling Group Launch Configuration

**User Story:** As a Tokeira operator, I want ASG instances to be configured with the ECS agent and proper IAM roles, so that they register with the ECS cluster automatically.

#### Acceptance Criteria

1. THE Auto_Scaling_Group instances SHALL use the Amazon Linux 2023 ECS-optimized AMI for arm64, resolved at apply time from the SSM public parameter `/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id`. This AMI supports all current Graviton generations (Graviton2, Graviton3, Graviton4).
2. THE Auto_Scaling_Group instances SHALL have an IAM instance profile with permissions for ECS agent registration, ECR image pull, CloudWatch Logs (if enabled by `tkr debug logs enable`), and SSM Session Manager (for `tkr port-forward` and `tkr exec`).
3. THE Auto_Scaling_Group instances SHALL have user data that configures the ECS agent to join the correct ECS_Cluster AND sets ECS instance attributes identifying the workload (`ECS_INSTANCE_ATTRIBUTES={"workload": "<plane>"}`) so ECS task placement constraints can filter by plane (`edge-api`, `edge-poll`, `runtime`, `projection`, `control`, `mimir`, `loki`, `grafana`).
4. ALL Auto_Scaling_Group instance types SHALL be Graviton4 (c8g/r8g family) or a generation the operator explicitly overrides in config. Defaults SHALL target Graviton4 where the family is generally available, with Graviton3 (c7g/r7g) as fallback in unsupported regions.

### Requirement 3.4: ECS Exec Support

**User Story:** As a Tokeira operator, I want `ecs exec` available on every running task with a central audit log, so that I can diagnose live issues without SSH, a bastion host, or public ingress.

#### Acceptance Criteria

1. THE ECS_Cluster SHALL configure `execute_command_configuration` with `logging = "OVERRIDE"` and a dedicated CloudWatch log group `/ecs/{project_name}/ecs-exec` capturing every exec session for audit.
2. THE `/ecs/{project_name}/ecs-exec` log group SHALL carry the auto-generated tags from Req 1.1a and SHALL use the configured `log_retention_days` setting (default 30 days).
3. EVERY ECS_Service defined by the ECS_Platform (edge-api, edge-poll, runtime, projection, controller, autoscaler, admin, mimir, loki, grafana) SHALL set `enable_execute_command = true`. A cluster-wide exec toggle does not exist; per-service opt-in is required.
4. EVERY primary container in every task definition SHALL set `linuxParameters.initProcessEnabled = true`. Without an init process, exec sessions can leave zombie processes when a shell exits.
5. EVERY task role (not the execution role) for services with exec enabled SHALL include an inline policy granting the four SSM Messages actions (`ssmmessages:CreateControlChannel`, `ssmmessages:CreateDataChannel`, `ssmmessages:OpenControlChannel`, `ssmmessages:OpenDataChannel`) and the three CloudWatch Logs actions needed to write session logs (`logs:CreateLogStream`, `logs:DescribeLogStreams`, `logs:PutLogEvents`), each with `Resource = "*"` or scoped to the ecs-exec log group ARN where supported.
6. THE VPC endpoints required for SSM Session Manager (`ssm`, `ssmmessages`, `ec2messages`) SHALL be provisioned as part of the required endpoint set in Req 2.2.
7. THE `tkr exec <service> [--container <name>] -- <cmd>` command SHALL open an ECS Exec session. It SHALL discover a running task via `ecs:ListTasks` + `ecs:DescribeTasks`, call `ecs:ExecuteCommand` with `interactive = true`, and hand off to `session-manager-plugin` for the tty data-plane — mirroring the `tkr port-forward` approach.
8. WHEN the operator omits `--container`, THE CLI SHALL default to the primary application container for the service (for example `tokeira-runtime` for the runtime service), not the Alloy sidecar.

---

## Feature 4: ECS Service Definitions

### Requirement 4.0: Canonical Service Port Assignments

**User Story:** As a Tokeira developer, I want every service's port assignments specified in one place, so that Service Connect registrations, wait-for init containers, port-forward defaults, and task definition port mappings all agree.

#### Acceptance Criteria

1. THE service port assignments SHALL be fixed as follows:

| Service | gRPC | Metrics | HTTP | Notes |
|---|---|---|---|---|
| `tokeira-edge-api` | 7233 | 9090 | — | ALB target group (non-poll) |
| `tokeira-edge-poll` | 7234 | 9090 | — | ALB target group (poll) |
| `tokeira-runtime` | 7235 | 9090 | — | Internal only (edge→runtime forwarding) |
| `tokeira-projection` | — | 9090 | — | No gRPC ingress; workers pull from projection_log |
| `tokeira-controller` | 7240 | 9090 | — | Service Connect (`tokeira-controller.<namespace>`) |
| `tokeira-autoscaler` | — | 9090 | — | No ingress; outbound only |
| `tokeira-admin` | 7250 | 9090 | — | On-demand gRPC for operator-driven schema ops |
| `tokeira-mimir` | 9095 | 9009 | 9009 | HTTP 9009 = ingest + Prom query; gRPC 9095 = internal |
| `tokeira-loki` | 9095 | 3100 | 3100 | HTTP 3100 = ingest + query; gRPC 9095 = internal |
| `tokeira-grafana` | — | 3000 | 3000 | HTTP UI; metrics scraped from same port |

2. Services with no gRPC column SHALL NOT register a `grpc` port on Service Connect.
3. EVERY service SHALL register a `metrics` port on Service Connect at the port listed above (Req 5.1.4).
4. ALL port numbers SHALL be configurable in `EcsConfig.services.<service>.{grpc_port, metrics_port, http_port}` — the table documents defaults. Changing a port in config SHALL propagate through task definitions, Service Connect registrations, wait-for init container commands, and port-forward defaults.
5. THE `tkr port-forward` default local-port mapping (Req 9.6.7) SHALL match the HTTP or gRPC column above where a single port is relevant: grafana=3000, edge-api=7233, edge-poll=7234, controller=7240, mimir=9009, loki=3100.

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
3. THE `tokeira-autoscaler` ECS_Service SHALL have IAM permissions for:
   - **ECS service scaling** (Loop A): `ecs:UpdateService`, `ecs:DescribeServices`, `ecs:ListServices`.
   - **ASG scaling** (Loop B): `autoscaling:SetDesiredCapacity`, `autoscaling:DescribeAutoScalingGroups`.
   - **Runtime retirement** (Loop C): `autoscaling:TerminateInstanceInAutoScalingGroup`, `autoscaling:SetInstanceProtection`, `ecs:UpdateContainerInstancesState`, `ecs:ListContainerInstances`, `ecs:DescribeContainerInstances` (to resolve `ec2_instance_id → container_instance_arn`), `ec2:DescribeInstances` (to confirm liveness before terminating).
   - **Read-only cluster introspection**: `ecs:DescribeClusters`, `ecs:ListTasks`, `ecs:DescribeTasks`.
   All permissions SHALL be scoped where the AWS API supports it (for example `ecs:UpdateService` scoped to the cluster's services). `Resource = "*"` is acceptable only for APIs that do not support resource-level conditions (e.g., `autoscaling:Describe*`).
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
4. THE `EcsConfig` loader SHALL validate each service's configured `cpu` against the ECS task-level CPU/memory matrix and reject invalid combinations at parse time with a descriptive error. Valid CPU units are `256`, `512`, `1024`, `2048`, `4096`, `8192`, `16384`. For each CPU value, memory SHALL fall within the ECS-documented range (for example, `cpu = 1024` requires `memory` in 2048–8192 MiB in 1024 MiB increments). The validator SHALL state the invalid pair and the nearest valid pairs in its error message.
5. EVERY primary container in every task definition SHALL declare a container-level `healthCheck` matching the service's traffic model:
   - gRPC services (edge-api, edge-poll, runtime, controller, admin) SHALL use `grpc_health_probe -addr=localhost:<grpc_port>` (or equivalent) with `interval=30s`, `timeout=5s`, `retries=3`, `startPeriod=60s`.
   - HTTP services (mimir, loki, grafana) SHALL use an HTTP `/ready` or `/-/ready` probe via `wget` / `curl` against the service's HTTP port.
   - Metrics-only services (projection, autoscaler) SHALL probe their metrics endpoint (`GET /metrics` returning 200) as a liveness proxy.
6. ECS service-level health check (ALB integration) SHALL be configured ONLY for edge-api and edge-poll (the services behind the ALB). THE ALB target group health check path and protocol SHALL match Req 2.3.5 (gRPC health check protocol, status 0 = healthy).

### Requirement 4.9: Startup-Order Readiness (Init Container Barrier)

**User Story:** As a Tokeira operator, I want each service's task to wait until its declared upstream dependencies are reachable on Service Connect before the primary container starts, so that rolling updates and ASG replacements do not produce spurious crash-loops from DNS propagation gaps.

#### Acceptance Criteria

1. FOR each service with declared upstream dependencies (per the service dependency graph in Feature 5.1), the generated Task_Definition SHALL include a `wait-for-<dep>` init container for each dependency.
2. EACH `wait-for-<dep>` init container SHALL use a minimal busybox/netcat image and poll the dependency's Service Connect endpoint at 2-second intervals until TCP connect succeeds, then exit 0.
3. THE primary container SHALL declare `dependsOn = [{ containerName = "wait-for-<dep>", condition = "SUCCESS" }]` for each init container.
4. EACH `wait-for-<dep>` init container SHALL be marked `essential = false` so its normal exit does not stop the task.
5. Init container CPU and memory reservations SHALL be accounted for in the task-level totals (typical: 32 CPU, 64 MiB per init container).
6. Init containers SHALL not be required for services with no declared upstream dependencies (for example `tokeira-controller`, `tokeira-mimir`, `tokeira-loki`, `tokeira-admin`).

---

## Feature 5: Service Discovery and Connectivity

### Requirement 5.1: Service Connect Configuration

**User Story:** As a Tokeira operator, I want Service Connect configured for both gRPC traffic and metrics scrape targets, so that services discover each other and Mimir can pull metrics over stable internal DNS instead of relying on Docker-socket-based discovery.

#### Acceptance Criteria

1. THE ECS_Platform SHALL configure a Service Connect namespace on the ECS_Cluster.
2. THE ECS_Platform SHALL enable Service Connect on all ECS services that need outbound service-to-service connectivity.
3. THE Service_Connect configuration SHALL register each service's primary gRPC port (where applicable) with `port_name = "grpc"` under the discovery name `<service>` (for example `tokeira-controller.<namespace>`).
4. THE Service_Connect configuration SHALL register each service's Prometheus metrics port (`9090`) with `port_name = "metrics"` under the discovery name `<service>-metrics` (for example `tokeira-runtime-metrics.<namespace>:9090`). This applies to every service that exposes metrics — including observability services that expose their own metrics.
5. Mimir SHALL discover scrape targets through the registered `-metrics` Service Connect DNS names rather than the Docker socket. Alloy sidecars continue to scrape `localhost:9090` and forward via remote-write; Service Connect metrics registration is an additional path that lets Mimir pull directly when the sidecar is unavailable.
6. THE Service_Connect configuration SHALL provide stable internal endpoints for: controller, projection, admin, mimir, loki, and grafana services.

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
5. IF a controller gRPC call (`NominateScaleInCandidates`, `MarkNodeDraining`) fails mid-Loop C, THEN THE Autoscaler SHALL defer the scale-in decision to the next loop iteration rather than aborting the reconciler state. Scale-out and scale-up decisions (Loop A, Loop B) SHALL NOT be blocked by controller unavailability.

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

1. THE module dependency graph SHALL be: `remote-state` → `networking` → `dsql` → `cluster` → `observability` → `services`. `cluster` SHALL depend on `dsql` (not just `networking`) so DSQL resources exist before the compute plane that references their IAM role ARNs.
2. THE module dependency graph SHALL be a DAG with no cycles.
3. THE `infra_modules` method SHALL return modules filtered by `ModuleSelection`.
4. WITHIN each module, IAM roles (task roles, instance profiles) SHALL be enumerated before the resources that reference them. THE IaC engine's intra-module resource ordering is `Resource::dependencies()`-driven per `iac-resource-lifecycle` — modules SHALL declare role → consumer dependencies explicitly rather than relying on enumeration order.

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

### Requirement 7.5: DSQL Module

**User Story:** As a Tokeira developer, I want a DSQL IaC module that can either provision a new Aurora DSQL cluster or adopt a pre-existing one, so that operators can choose between a fully-managed lifecycle and reusing a cluster shared with other projects.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `dsql` IaC module implementing the `Module` trait.
2. THE `dsql` module SHALL depend on the `networking` module (DSQL PrivateLink endpoints require VPC resources).
3. THE `EcsConfig.dsql` section SHALL expose a `mode` field with variants `Managed` and `Preexisting`, defaulting to `Managed`.
4. WHEN `dsql.mode == Managed`, THE `dsql` module SHALL enumerate and create: one DSQL cluster, two DSQL PrivateLink endpoints (management and connection), and two IAM authentication roles (runtime and admin).
5. WHEN `dsql.mode == Preexisting`, THE operator SHALL supply `endpoint`, `management_endpoint_id`, `connection_endpoint_id`, `runtime_role_arn`, and `admin_role_arn` in config. THE module SHALL adopt those resources without calling any create or delete AWS API, following the `effective_managed` convention from the `iac-resource-lifecycle` spec.
6. WHEN `dsql.mode == Preexisting` AND any required field is missing, THE config loader SHALL reject the config with an error naming the missing field.
7. THE `dsql` module resources SHALL implement the `Resource` trait. Each resource SHALL persist its mode under `ResourceState.properties["mode"]` so a cluster originally created as `Managed` is still deleted on destroy even if the config is later changed to `Preexisting`.

#### Acceptance Criteria (continued)

8. WHEN `dsql.mode == Managed`, THE runtime IAM role created by the `dsql` module SHALL have:
   - Trust policy: `ecs-tasks.amazonaws.com` as the service principal.
   - Inline policy: `dsql:DbConnect` on `arn:aws:dsql:{region}:{account}:cluster/{cluster_id}`.
9. WHEN `dsql.mode == Managed`, THE admin IAM role SHALL have:
   - Trust policy: `ecs-tasks.amazonaws.com` as the service principal.
   - Inline policy: `dsql:DbConnectAdmin` on the DSQL cluster ARN (superset of the runtime role).
10. WHEN `dsql.mode == Preexisting`, THE module SHALL NOT create IAM roles — it consumes the ARNs the operator supplied in config.
11. THE runtime and admin role ARNs SHALL be exposed to downstream modules via `ResourceState.properties["role_arn"]` so `ServicesModule` can wire them as task roles without re-reading config.

### Requirement 7.5a: DSQL Config Hydration and Writeback

**User Story:** As a Tokeira operator, I want Managed-mode DSQL values to be available through two paths — hydrated from state on every engine construction, and written back to the deployment config file after apply — so that downstream commands work correctly even if writeback is missed, and operators can read the config file to see discovered values.

#### Acceptance Criteria

1. THE `EcsDeployment::hydrate_config(config, state)` method SHALL populate empty DSQL fields (`endpoint`, `management_endpoint_id`, `connection_endpoint_id`, `runtime_role_arn`, `admin_role_arn`) from `InfraState` on every engine construction. Hydration is the source of truth during `deploy apply`, `deploy plan`, `scale up`, and `schema setup`; it guarantees correctness even when config-file writeback is missed.
2. THE `EcsDeployment::collect_writeback(config, state)` method SHALL return `(dotted_key, value)` pairs for discovered DSQL fields that differ from the current config, so the CLI can persist them to `deployment.toml` via `toml_edit`. Writeback is a convenience for operators reading the file; it is NOT the correctness path.
3. WHEN `dsql.mode == Managed` AND `hydrate_config` cannot find the corresponding resource in state, `EcsDeployment::services()` and `EcsDeployment::images()` SHALL return an error stating `infra apply has not run successfully; DSQL endpoint is not yet known`. This prevents `deploy apply` from using placeholder values.
4. WHEN a Managed DSQL module is destroyed (removed from state), THE `EcsDeployment::collect_destroy_writeback(config, state, active_modules)` method SHALL return pairs that CLEAR the corresponding DSQL config fields (empty string or `None`), so subsequent `infra plan` does not treat destroyed resources as Preexisting.
5. THE CLI SHALL call `collect_writeback` after `infra apply` and `collect_destroy_writeback` after `infra destroy`, writing the returned pairs via `toml_edit` (preserving comments and formatting, per the `iac-resource-lifecycle` config writeback requirement).
6. FOR ALL valid `InfraState` values, `hydrate_config(config, state)` SHALL be idempotent: applying it twice produces the same result as applying it once.

---

## Feature 8: Observability Stack

### Requirement 8.1: Alloy Sidecar in Task Definitions

**User Story:** As a Tokeira operator, I want an Alloy sidecar container in each Tokeira task definition, so that metrics are scraped from localhost and logs are shipped to Loki without requiring a separate log driver.

#### Acceptance Criteria

1. EACH Tokeira-owned ECS Task_Definition (edge-api, edge-poll, runtime, projection, controller, autoscaler, admin, mimir, loki, grafana) SHALL include an Alloy_Sidecar container alongside the primary application container. THE observability services' Alloy sidecars ship only logs to Loki — their own metrics are scraped via the `-metrics` Service Connect alias by Mimir itself (except for the Mimir service, whose self-metrics Alloy forwards directly to Mimir's `/api/v1/push`).
2. THE Alloy_Sidecar SHALL scrape Prometheus metrics from the primary container's metrics port on `localhost`.
3. THE Alloy_Sidecar SHALL forward scraped metrics to the Mimir remote-write endpoint (`/api/v1/push`) over the Service Connect endpoint `mimir.<namespace>:9009`.
4. THE Alloy_Sidecar SHALL collect container stdout/stderr logs and ship them to the Loki push endpoint (`/loki/api/v1/push`) over the Service Connect endpoint `loki.<namespace>:3100`.
5. THE Alloy_Sidecar SHALL read its configuration from a shared task volume populated by an init container, NOT from environment variables. The init container (`alloy-config-init`) SHALL fetch the per-service Alloy config from SSM Parameter Store at path `/{project_name}/alloy/sidecar/{service_name}` and write it to a `alloy-config` volume shared with the Alloy container. This lets operators update Alloy configuration by writing a new SSM parameter value without registering a new task definition revision.
6. THE SSM Parameter Store path `/{project_name}/alloy/sidecar/*` SHALL be readable by the execution role (so the init container can fetch the config) and writable by operator/admin principals only (not by task roles). The parameter content SHALL be the full Alloy HCL config rendered from an Askama template at infra-apply time.
7. THE Alloy_Sidecar SHALL use a pinned image version matching the compose platform (`grafana/alloy:v1.16.0`).
8. THE Alloy_Sidecar SHALL have resource limits configured separately from the primary container (CPU and memory).
9. THE `alloy-config-init` init container SHALL be marked `essential = false` and SHALL exit 0 after writing the config file. The Alloy sidecar SHALL declare `dependsOn = [{ containerName = "alloy-config-init", condition = "SUCCESS" }]`.

### Requirement 8.2: Mimir ECS Service

**User Story:** As a Tokeira operator, I want Mimir deployed as a dedicated ECS service, so that ingestion load does not compete with other control-plane tasks and Prometheus queries stay responsive.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-mimir` ECS_Service with REPLICA scheduling on a dedicated `cp-mimir` capacity provider.
2. THE `tokeira-mimir` Task_Definition SHALL specify the Mimir container image (pinned: `grafana/mimir:3.0.6`), gRPC and HTTP ports, resource limits, and S3 storage configuration.
3. THE `tokeira-mimir` ECS_Service SHALL use Service_Connect for service-to-service traffic with a stable internal endpoint (`mimir.tokeira.local`).
4. THE `tokeira-mimir` ECS_Service SHALL have an IAM role with S3 read/write permissions for the metrics storage bucket.
5. THE Mimir configuration SHALL use S3 as the long-term storage backend (single-binary mode).

### Requirement 8.3: Loki ECS Service

**User Story:** As a Tokeira operator, I want Loki deployed as a dedicated ECS service, so that log ingestion and query load do not compete with other control-plane tasks.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-loki` ECS_Service with REPLICA scheduling on a dedicated `cp-loki` capacity provider.
2. THE `tokeira-loki` Task_Definition SHALL specify the Loki container image (pinned: `grafana/loki:3.7.1`), HTTP port, resource limits, and S3 storage configuration.
3. THE `tokeira-loki` ECS_Service SHALL use Service_Connect for service-to-service traffic with a stable internal endpoint (`loki.tokeira.local`).
4. THE `tokeira-loki` ECS_Service SHALL have an IAM role with S3 read/write permissions for the log storage bucket.
5. THE Loki configuration SHALL use S3 as the long-term storage backend (single-binary mode).

### Requirement 8.4: Grafana ECS Service

**User Story:** As a Tokeira operator, I want Grafana deployed as a dedicated ECS service, so that dashboard rendering does not compete with other control-plane tasks.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-grafana` ECS_Service with REPLICA scheduling on a dedicated `cp-grafana` capacity provider.
2. THE `tokeira-grafana` Task_Definition SHALL specify the Grafana container image (pinned: `grafana/grafana-oss:12.4.3`), HTTP port, resource limits, and data source configuration.
3. THE `tokeira-grafana` ECS_Service SHALL be pre-configured with Mimir and Loki as data sources.
4. THE `tokeira-grafana` ECS_Service SHALL be reachable via `tkr port-forward grafana` (SSM Session Manager port forwarding). THE Grafana service SHALL NOT be registered with the internal ALB.
5. THE Grafana admin credentials SHALL be sourced from Secrets Manager, not hardcoded.
6. THE `observability` module SHALL create a Secrets Manager secret named `{project_name}/grafana/admin` on first apply. THE secret SHALL contain a JSON value `{ "username": "admin", "password": "<random-32-char>" }` generated via `secretsmanager:GenerateRandomPassword` at create time. THE module SHALL NOT regenerate the password on subsequent applies — `update()` is a no-op on secret value. Rotation is operator-driven via `secretsmanager:PutSecretValue` (or a future `tkr rotate-grafana-admin` command).
7. THE Grafana ECS_Service's task role SHALL have `secretsmanager:GetSecretValue` scoped to the secret ARN. The ECS task definition SHALL reference the secret via `containerDefinitions.secrets` so ECS injects the password as an environment variable (`GRAFANA_ADMIN_PASSWORD`) at task start, never logging the value.

### Requirement 8.5: Observability IaC Module

**User Story:** As a Tokeira developer, I want an observability IaC module that provisions Mimir, Loki, and Grafana services, so that the observability stack is managed as infrastructure alongside the application services.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define an `observability` IaC module implementing the `Module` trait.
2. THE `observability` module SHALL depend on the `cluster` module (observability services need the ECS cluster and Service Connect namespace to be provisioned first).
3. THE `observability` module SHALL enumerate resources for: S3 buckets for Mimir and Loki storage, IAM roles for S3 access, Mimir/Loki/Grafana task definitions and ECS services.
4. THE `observability` module resources SHALL implement the `Resource` trait.

### Requirement 8.6: Observability Configuration

**User Story:** As a Tokeira operator, I want observability stack settings in the ECS config, so that I can customize image versions, resource limits, retention, and storage settings.

#### Acceptance Criteria

1. THE `EcsConfig` SHALL include an `observability` section with fields for: Mimir image and resource limits, Loki image and resource limits, Grafana image and resource limits, Alloy sidecar image and resource limits, S3 bucket names for metrics and log storage, and `retention_days`.
2. THE `observability` section SHALL have sensible defaults matching the pinned versions from the compose platform.
3. THE `observability.retention_days` field SHALL default to `30` and SHALL apply to both Mimir metrics and Loki logs.
4. THE `observability` section SHALL be optional — if omitted, the observability stack is not deployed, but Alloy sidecars are still included in task definitions (they will fail to connect until Mimir/Loki are available).

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
3. THE `tkr scale up` command SHALL scale services in startup order, waiting for each to reach ready state before proceeding to the next: mimir → loki → grafana → controller → runtime → edge-api → edge-poll → projection → autoscaler. Rationale: observability first so autoscaler has a metrics backend on start; controller before runtime so runtime tasks can renew membership leases and publish endpoints on their first startup; edge services after runtime so `wait-for-tokeira-runtime` init containers unblock promptly; autoscaler last because it needs everything else before making any scaling decision.
4. THE `tkr scale up` command SHALL show the operator the planned scaling actions and require confirmation.

### Requirement 9.6: Service Port-Forwarding

**User Story:** As a Tokeira operator, I want to reach private services (Grafana, edge APIs, controller) from my workstation without exposing them publicly, so that I can operate a fully private deployment.

#### Acceptance Criteria

1. THE `tkr` CLI SHALL support `tkr port-forward <service> [--local-port <port>]` for services: `grafana`, `edge-api`, `edge-poll`, `controller`, `mimir`, `loki`.
2. THE `port-forward` command SHALL open a tunnel from the operator's workstation through AWS Systems Manager Session Manager to an ECS container instance in the private subnets, then to the target ECS service port. No internet gateway, bastion host, or VPN is required.
3. THE `port-forward` command SHALL discover a running ECS container instance for the target service's capacity provider via `ecs:ListContainerInstances` and `ecs:DescribeContainerInstances`.
4. THE `port-forward` command SHALL invoke `ssm start-session` with document `AWS-StartPortForwardingSession` (or `AWS-StartPortForwardingSessionToRemoteHost` when the target is the Service Connect endpoint on a different task).
5. THE Auto_Scaling_Group IAM instance profiles SHALL include the `AmazonSSMManagedInstanceCore` managed policy so Session Manager can establish tunnels.
6. THE `port-forward` command SHALL stream connection status to stdout and exit cleanly on Ctrl-C.
7. WHEN the operator omits `--local-port`, THE CLI SHALL choose a default per service (`grafana=3000`, `edge-api=7233`, `edge-poll=7234`, `controller=7240`, `mimir=9009`, `loki=3100`) — matching the canonical ports in Req 4.0.
8. `tkr port-forward` and `tkr exec` are interactive tty commands. WHEN `--json` is active, THE CLI SHALL emit a single-line `SessionStarted { service, session_id, target_instance_id, started_at_ms }` JSON event before handing off to `session-manager-plugin`, and a single-line `SessionEnded { session_id, elapsed_ms, exit_code }` JSON event after the plugin exits. Stream content from the plugin is passed through unmodified because it is already tty data. The operator's local-port connection SHALL be logged at `tracing::info` but SHALL NOT be emitted as JSON (it is host-state, not a session event).

### Requirement 9.7: Admin Service On-Demand Execution

**User Story:** As a Tokeira operator, I want to run schema migrations and diagnostic commands against the admin service without managing its lifecycle manually, so that one-shot ops don't require multiple `tkr scale` invocations.

#### Acceptance Criteria

1. THE `tkr admin <subcommand>` command SHALL scale the `tokeira-admin` ECS service from 0 to 1, wait for the task to reach RUNNING with passing health check, execute the subcommand via `ecs:ExecuteCommand` (same plumbing as `tkr exec`), stream output, then scale back to 0.
2. THE supported subcommands SHALL include at minimum: `schema setup`, `schema migrate <version>`, `schema status`, and `diagnostics <target>`. The canonical subcommand list lives in the admin service binary's clap command tree; the CLI SHALL pass through unknown subcommands as-is.
3. IF the admin task fails to reach RUNNING within the configured timeout (default 120s), THEN THE CLI SHALL return an error including the task's `stoppedReason` and SHALL NOT scale the service back up on retry — the operator must re-invoke.
4. THE scale-back-to-0 step SHALL happen in a `finally`-equivalent block so a Ctrl-C mid-command still scales the service down.
