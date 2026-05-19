# Implementation Plan: ECS Deployment

## Overview

Implement the ECS on EC2 deployment infrastructure for Tokeira. The work is organized into 6 phases: platform crate scaffold, networking module, cluster module, services module, autoscaler service, and CLI integration. Each phase builds on the previous one. Two new crates are introduced: `platforms/ecs/` (platform) and `crates/tokeira-autoscaler/` (autoscaler library). A new binary entry point is added at `apps/tokeira-autoscaler/`.

## Tasks

- [ ] 1. Phase 1 — ECS Platform Crate and Configuration
  - [x] 1.1 Create `platforms/ecs/` crate scaffold
    - Create `platforms/ecs/Cargo.toml` with dependencies: `tokeira-iac`, `tokeira-deploy-engine`, `tokeira-orchestrator`, `tokeira-config`, `tokeira-aws`, `tokeira-state`, `tokeira-types`, `serde`, `async-trait`, `tracing`, `anyhow`
    - Create `platforms/ecs/src/lib.rs` with module declarations: `config`, `modules`, `services`
    - Add `platforms/ecs` to workspace `Cargo.toml` members
    - _Requirements: 1.2.1_

  - [x] 1.2 Implement ECS configuration model in `platforms/ecs/src/config.rs`
    - Define `EcsConfig` with sections: `ClusterConfig`, `NetworkingConfig`, `CapacityProviderConfigs` (8 fields: edge_api, edge_poll, runtime, projection, control, mimir, loki, grafana), `ServiceConfigs`, `AutoscalerConfig`, `AlbConfig`, `DsqlConfig`
    - Define `DsqlConfig` with `mode: DsqlClusterMode` (Managed/Preexisting, default Managed), `endpoint: Option<String>`, `management_endpoint_id: Option<String>`, `connection_endpoint_id: Option<String>`, `runtime_role_arn: Option<String>`, `admin_role_arn: Option<String>`
    - Define `CapacityProviderConfig` and `RuntimeCapacityProviderConfig` (with `scale_in_protection` field)
    - Define `ReplicaServiceConfig { image, desired_count, cpu, memory_mb, grpc_port: Option<u16>, metrics_port: u16, http_port: Option<u16> }` and `DaemonServiceConfig` (same fields minus desired_count)
    - Define `AlbConfig { name, listener_protocol: "http2" | "https", certificate_arn: Option<String>, health_check_path, health_check_interval_secs }` — `certificate_arn` required when `listener_protocol = "https"` (validated in `validate()`)
    - Define `OptionalEndpoints` struct for optional VPC endpoints
    - Use `serde(deny_unknown_fields)` on all config structs
    - Implement `Default` for `EcsConfig` with sensible defaults for a single-environment private-only deployment
    - Implement `validate()` method that checks: invalid combinations (empty subnet list, zero capacity), ECS cpu/memory matrix, DSQL Preexisting field completeness, HTTPS listener requires certificate_arn, service ports match Req 4.0 canonical assignments
    - _Requirements: 1.1.1, 1.1.2, 1.1.3, 1.1.4, 1.1.5, 4.0.1, 4.0.4, 2.3.8_

  - [x] 1.3 Add `PlatformKind::Ecs` variant to `tokeira-orchestrator`
    - Add `Ecs` variant to `PlatformKind` enum in `tokeira-orchestrator/src/lib.rs`
    - Add `Ecs` variant to `CliPlatformKind` in `tkr/src/cli.rs` (if separate)
    - _Requirements: 1.2.5, 8.1.1_

  - [x] 1.4 Implement `PlatformConfig` trait for `EcsDeployment`
    - Implement `prototypical_config()` returning default `EcsConfig` as TOML
    - Implement `prototypical_server_config()` returning `TokeiraConfig` with DSQL endpoint placeholder
    - _Requirements: 1.2.4, 8.2.1, 8.2.2, 8.2.3_

  - [x] 1.5 Implement stub `Deployment` and `Ops` traits for `EcsDeployment`
    - Implement all `Deployment` trait methods with minimal stubs (empty module lists, empty service lists)
    - Implement all `Ops` trait methods with the valid services list and stub implementations
    - Wire `remote_state_module` to use S3 state backend via `tokeira-aws` S3 resource
    - Wire `create_infra_store` and `create_deploy_store` to use S3 state backend
    - _Requirements: 1.2.2, 1.2.3_

  - [ ]* 1.6 Write property tests for ECS config
    - **Property 1: Config TOML round-trip** — serialize default config to TOML, deserialize, assert equality
    - **Property 2: Unknown fields rejected** — insert unknown key into valid TOML, assert deserialization fails
    - Use `proptest` crate
    - _Validates: Requirements 1.1.3, 1.3.1, 1.3.2_

  - [ ]* 1.7 Write unit tests for ECS platform scaffold
    - Test default config is valid (passes `validate()`)
    - Test `PlatformConfig::prototypical_config()` produces parseable TOML
    - Test `valid_services()` returns 10 service names (7 Tokeira + 3 observability). Observability names are valid targets for `tkr logs`, `tkr port-forward`, and `tkr exec`, but `tkr scale` on an observability service returns an error because observability desired-count is managed by the `observability` module, not the operator
    - Test invalid service name produces error listing valid alternatives
    - **Property 10: Invalid service rejection** — generate random strings not in valid set, verify error
    - _Requirements: 1.1.4, 9.3.3_

  - [x] 1.8 Implement DSQL hydration and writeback in `EcsDeployment`
    - Implement `hydrate_config(config, state) -> EcsConfig` that fills empty `dsql.{endpoint, management_endpoint_id, connection_endpoint_id, runtime_role_arn, admin_role_arn}` from `InfraState` per the pattern in design §6c. Idempotent (field-level `is_none()` guard)
    - Implement `collect_writeback(config, state) -> Vec<(String, String)>` that returns the subset of DSQL fields where hydrated values differ from current config
    - Extend `collect_writeback(config, state) -> Vec<(String, String)>` so it clears DSQL config fields when Managed-mode DSQL resources are absent from the post-destroy state
    - In `services()`, after hydration, if `dsql.mode == Managed` and `dsql.endpoint.is_none()`, return `Err` with the message `infra apply has not run successfully; DSQL endpoint is not yet known`
    - Wire `collect_writeback` into both the CLI's `infra apply` and `infra destroy` flows (the CLI already uses `toml_edit` for writeback per `iac-resource-lifecycle`)
    - _Requirements: 7.5a.1, 7.5a.2, 7.5a.3, 7.5a.4, 7.5a.5, 7.5a.6_

  - [ ]* 1.9 Write property tests for DSQL hydration
    - **Property 11: DSQL hydration idempotency** — generate random `EcsConfig` + `InfraState`, apply `hydrate_config` twice, assert equal
    - **Property 12: Deploy-apply guard** — generate `Managed` configs with the DSQL cluster resource absent from state, assert `services()` returns an error matching the expected message
    - _Requirements: 7.5a.3, 7.5a.6_

- [ ] 2. Checkpoint — Phase 1 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.
  - Run `cargo lint` to verify compilation.

- [x] 3. Phase 2 — Networking IaC Module
  - [x] 3.1 Implement security group resources in `platforms/ecs/src/modules.rs`
    - Define security group resources for: ALB, edge services, runtime services, control services, VPC endpoints
    - Each security group resource implements the `Resource` trait using `tokeira-aws` `SecurityGroupResource`
    - Security groups allow only minimum required ingress/egress per component
    - No `0.0.0.0/0` ingress rules
    - _Requirements: 2.4.1, 2.4.2, 2.4.3_

  - [x] 3.2 Implement VPC endpoint resources
    - Define a helper function `required_vpc_endpoints(region)` returning the 11 required generic endpoints: ECS(3), ECR(2), S3(gw), AutoScaling, CloudMap, SSM Session Manager(3: `ssm`, `ssmmessages`, `ec2messages`). DSQL endpoints are owned by the DSQL module.
    - Define a helper function `optional_vpc_endpoints(config, region)` returning enabled optional endpoints
    - Each VPC endpoint resource implements the `Resource` trait using `tokeira-aws` `VpcEndpointResource`
    - _Requirements: 2.2.1, 2.2.2, 2.2.3, 2.2.4, 2.2.5, 2.2.6, 2.2.7, 2.2.8_

  - [x] 3.3 Implement internal ALB resources
    - Define ALB resource, target group resources (edge-api, edge-poll), and listener resource
    - ALB is internal-only, placed in private subnets
    - Target groups support split private DNS names: `edge-api.<zone>` and `edge-poll.<zone>`
    - Implement as `Resource` trait implementations in `tokeira-aws` (new resource types: `AlbResource`, `AlbTargetGroupResource`, `AlbListenerResource`)
    - _Requirements: 2.3.1, 2.3.2, 2.3.3, 2.3.4_

  - [x] 3.4 Implement `NetworkingModule`
    - Define `NetworkingModule` implementing `Module` trait
    - `name()` returns `"networking"`, `dependencies()` returns `&["remote-state"]`
    - `resources()` enumerates: security groups, VPC endpoints, ALB resources
    - _Requirements: 7.1.1, 7.1.2, 7.1.3, 7.1.4_

  - [x]* 3.5 Write unit tests for networking module
    - Test `NetworkingModule` returns correct name and dependencies
    - Test resource enumeration includes all required VPC endpoints
    - Test optional endpoints are included only when enabled in config
    - Test security group resources report correct module name
    - _Requirements: 2.2, 2.4, 7.1_

- [ ] 4. Checkpoint — Phase 2 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [x] 4.5 Phase 2.5 — DSQL IaC Module
  - [x] 4.5.1 Implement `DsqlModule` in `platforms/ecs/src/modules.rs`
    - Define `DsqlModule` implementing `Module` trait, `name()` returns `"dsql"`, `dependencies()` returns `&["networking"]`
    - `resources()` enumerates: `DsqlClusterResource` (from `tokeira-aws`, mode-aware per Req 7.5), `DsqlPrivatelinkEndpointResource` ×2 (management + connection), `IamRoleResource` ×2 (runtime + admin roles, mode-aware)
    - When `dsql.mode == Preexisting`, enumerate adopter resources that take ARNs from config and never call AWS create/delete
    - `ResourceId` scheme: `dsql:cluster`, `dsql:management-endpoint`, `dsql:connection-endpoint`, `dsql:runtime-role`, `dsql:admin-role` — must match the IDs `hydrate_config` reads from state
    - _Requirements: 7.5.1, 7.5.2, 7.5.3, 7.5.4, 7.5.5, 7.5.6, 7.5.7_

  - [x] 4.5.2 Implement DSQL IAM role trust policies and DSQL action permissions
    - Runtime role (Managed mode): trust policy for `ecs-tasks.amazonaws.com`; inline policy granting `dsql:DbConnect` on the DSQL cluster ARN
    - Admin role (Managed mode): trust policy for `ecs-tasks.amazonaws.com`; inline policy granting `dsql:DbConnectAdmin` on the DSQL cluster ARN
    - Role ARNs exposed via `ResourceState.properties["role_arn"]` so `ServicesModule` can wire them as task roles
    - _Requirements: 7.5.8, 7.5.9, 7.5.10, 7.5.11_

  - [x]* 4.5.3 Write unit tests for DSQL module
    - Test `DsqlModule` returns correct name and dependencies
    - Test Managed mode enumerates 5 resources
    - Test Preexisting mode enumerates adopter variants (no create/delete calls in create())
    - Test Preexisting mode rejects config with any of the 5 required fields missing
    - Test IAM role ARN is exposed via `ResourceState.properties["role_arn"]`
    - _Requirements: 7.5_

- [ ] 4.6 Checkpoint — Phase 2.5 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [x] 5. Phase 3 — Cluster IaC Module
  - [x] 5.1 Implement new AWS resource types in `tokeira-aws`
    - Add `EcsClusterResource` implementing `Resource` trait. On create, configure `execute_command_configuration.logging = "NONE"` and set the Service Connect default namespace. Do not enable Container Insights by default; CloudWatch integrations remain optional debug features.
    - Add `LaunchTemplateResource` implementing `Resource` trait. The AMI is resolved at apply time via SSM parameter `/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id`. User data includes `ECS_INSTANCE_ATTRIBUTES={"workload": "<plane>"}` so task placement constraints can filter by plane
    - Add `AsgResource` implementing `Resource` trait (create/update/delete ASG with instance protection config)
    - Add `CapacityProviderResource` implementing `Resource` trait. For `cp-runtime`, `managed_termination_protection = "DISABLED"` despite per-instance protection (DAEMON services do not satisfy the ECS precondition for capacity-provider-managed termination protection; safety comes from Loop C). For all other providers, also `DISABLED` so they can scale to zero.
    - Add `IamInstanceProfileResource` implementing `Resource` trait. Role policies include: ECS agent registration, ECR image pull, `AmazonSSMManagedInstanceCore` (for SSM Session Manager — port-forward and exec), and conditionally CloudWatch Logs (`tkr debug logs enable`)
    - _Requirements: 3.1.1, 3.2.1, 3.2.2, 3.2.3, 3.2.4, 3.2.7, 3.3.1, 3.3.2, 3.3.3, 3.4.1, 3.4.2, 3.4.6_

  - [x] 5.2 Implement `ClusterModule`
    - Define `ClusterModule` implementing `Module` trait
    - `name()` returns `"cluster"`, `dependencies()` returns `&["dsql"]` (not `"networking"` — cluster depends on DSQL roles being available before instance profiles reference them)
    - `resources()` enumerates: ECS cluster (with exec configuration), 8 IAM instance profiles, 8 launch templates (one per plane, with workload attribute in user data), 8 ASGs, 8 capacity providers
    - Runtime ASG has `NewInstancesProtectedFromScaleIn: true`; runtime capacity provider has `managed_termination_protection = "DISABLED"` (DAEMON services do not satisfy ECS's precondition for managed termination protection — per-instance protection plus Loop C is the safety mechanism)
    - Control-plane capacity provider has `max_capacity >= 3` for rolling-update headroom
    - Observability ASGs (mimir, loki, grafana) each run with min=1, max=1, desired=1 so each observability service has a unique host (enables deterministic `tkr port-forward` targeting)
    - _Requirements: 3.1.1, 3.1.2, 3.1.3, 3.2.1, 3.2.2, 3.2.3, 3.2.4, 3.2.5, 3.2.6, 3.2.7, 3.2.8, 3.3.3, 3.4.1, 3.4.2, 7.2.1, 7.2.2, 7.2.3, 7.2.4_

  - [x]* 5.3 Write unit tests for cluster module
    - Test `ClusterModule` returns correct name and dependencies
    - Test resource enumeration includes ECS cluster, 8 CPs, 8 ASGs
    - Test runtime ASG has per-instance scale-in protection enabled
    - Test `cp-runtime` capacity provider has `managed_termination_protection = "DISABLED"` and `cp-runtime` ASG has `new_instances_protected_from_scale_in = true`
    - Test observability ASGs (`cp-mimir`, `cp-loki`, `cp-grafana`) have `max_size = 1, desired_capacity = 1`
    - Test `cp-control` capacity provider has `max_size >= 3` for rolling-update headroom
    - Test each launch template's user data contains the correct `ECS_INSTANCE_ATTRIBUTES` workload value
    - Test the ECS cluster's `execute_command_configuration.logging` is `NONE`
    - Test all resources report correct module name
    - **Property 3: Module DAG** — verify module dependency graph is acyclic
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 7.2, 7.4_

- [ ] 6. Checkpoint — Phase 3 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [ ] 7. Phase 4 — Services IaC Module and Deploy-Engine Integration
  - [x] 7.1 Implement ECS service and task definition resource types in `tokeira-aws`
    - Add `TaskDefinitionResource` implementing `Resource` trait (register/deregister task definition). The primary container MUST declare `linuxParameters.initProcessEnabled = true` so ECS Exec sessions exit cleanly
    - Add `EcsServiceResource` implementing `Resource` trait. MUST set `enable_execute_command = true` on every service
    - Add `CloudMapNamespaceResource` implementing `Resource` trait (create/delete private DNS namespace)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 5.2.1, 3.4.3, 3.4.4_

  - [x] 7.2 Implement `ServicesModule`
    - Define `ServicesModule` implementing `Module` trait
    - `name()` returns `"services"`, `dependencies()` returns `&["observability"]`
    - `resources()` enumerates: Cloud Map namespace, 7 task definitions (each with primary container + `alloy-config-init` init container + Alloy sidecar + `docker-sock` host-path volume + wait-for-<dep> init containers for each declared upstream dependency), 7 ECS services
    - Primary containers do not set `logConfiguration` in the normal task definition; they use Docker's default json-file logs so Alloy can collect stdout/stderr through the Docker socket
    - Each primary container sets `linuxParameters.initProcessEnabled = true`
    - Each ECS service sets `enable_execute_command = true`
    - Each ECS service's Service Connect configuration registers both `grpc` (where applicable) and `metrics` ports with discovery names `<service>` and `<service>-metrics`
    - Each ECS service declares `placement_constraints = [memberOf attribute:workload == <plane>]` matching the plane of its capacity provider
    - Runtime service registers in Cloud Map namespace
    - _Requirements: 3.4.3, 3.4.4, 4.9.1, 4.9.2, 4.9.3, 4.9.4, 4.9.5, 5.1.1, 5.1.2, 5.1.3, 5.1.4, 5.1.5, 5.1.6, 5.2.1, 5.2.2, 5.2.3, 7.3.1, 7.3.2, 7.3.3, 7.3.4, 8.1.1, 8.1.2, 8.1.3, 8.1.4, 8.1.5, 8.1.6, 8.1.7, 8.1.8, 8.1.9, 8.1.10_

  - [x] 7.3 Implement `EcsWorkload` deploy-engine service adapter
    - Define `EcsWorkload` struct with `name`, `scheduling` (REPLICA/DAEMON), `capacity_provider`, `task_definition`, `service_connect: ServiceConnectSpec`, `placement_constraints: Vec<PlacementConstraint>`
    - Extend `TaskDefinitionSpec` with `init_containers: Vec<InitContainerSpec>`, `init_process_enabled: bool`, and `health_check: HealthCheck` (mandatory per Req 4.8.5)
    - Define `ServiceConnectSpec { grpc: Option<ServiceConnectPort>, metrics: Option<ServiceConnectPort> }` and populate `metrics` for every service (port from Req 4.0 canonical port table, discovery name `<service>-metrics`)
    - Implement `deploy_engine::Service` trait: `name()`, `module()`, `dependencies()`, `manifests()`
    - Define service dependencies (match statement): edge-api/poll depend on runtime+controller, runtime depends on controller (for membership lease renewal and endpoint publishing), projection depends on runtime, autoscaler depends on controller+mimir, grafana depends on mimir+loki
    - `EcsWorkload::build(config)` synthesises one `wait-for-<dep>` init container per declared upstream dependency: busybox image, command `until nc -z <dep>.<namespace> <port>; do sleep 2; done`, `essential = false`, `cpu = 32`, `memory_mb = 64`. The primary container's `dependsOn` references each init container with `condition = "SUCCESS"`
    - The primary container's CPU/memory reservations SHALL subtract the init container and sidecar allocations from the task-level totals so reservations sum correctly
    - Generate task definition JSON as manifest
    - _Requirements: 4.0.1, 4.0.2, 4.0.3, 4.0.4, 4.8.1, 4.8.2, 4.8.3, 4.8.5, 4.9.1, 4.9.2, 4.9.3, 4.9.4, 4.9.5, 4.9.6_

  - [x] 7.4 Implement `EcsImage` deploy-engine image adapter
    - Define `EcsImage` struct wrapping ECR image references
    - Implement `deploy_engine::Image` trait
    - _Requirements: 4.1.2, 4.2.2, 4.3.2, 4.4.2, 4.5.2, 4.6.2, 4.7.2_

  - [x] 7.5 Wire services into `EcsDeployment::services()` and `EcsDeployment::images()`
    - Generate all 7 `EcsWorkload` instances from `EcsConfig`
    - Generate all `EcsImage` instances from `EcsConfig`
    - Verify scheduling types: DAEMON for runtime, REPLICA for all others
    - Verify capacity provider assignments match the design
    - _Requirements: 4.1.1, 4.2.1, 4.3.1, 4.4.1, 4.5.1, 4.6.1, 4.7.1_

  - [x] 7.6 Wire `infra_modules` to return all six modules in dependency order
    - Update `EcsDeployment::infra_modules()` to return `NetworkingModule`, `DsqlModule`, `ClusterModule`, `ObservabilityModule`, `ServicesModule` filtered by `ModuleSelection` (plus `RemoteStateModule` from `remote_state_module()`)
    - Verify module dependency ordering: `remote-state → networking → dsql → cluster → observability → services`
    - _Requirements: 7.4.1, 7.4.2, 7.4.3_

  - [x] 7.7 Add task CPU/memory validation to `EcsConfig::validate()`
    - Implement `validate_cpu_memory(cpu: u32, memory_mb: u32) -> Result<(), ConfigError>` using the ECS matrix described in design §1a CPU/Memory Validation
    - Call it for every service definition (`edge_api`, `edge_poll`, `runtime`, `projection`, `controller`, `autoscaler`, `admin`, `mimir`, `loki`, `grafana`)
    - Error messages SHALL name the invalid pair and the nearest valid pairs so operators can correct without consulting AWS docs
    - Call `validate()` from `tokeira-config::load_config` so invalid configs are rejected at `tkr init` / `tkr infra plan`, not at `ecs:RegisterTaskDefinition`
    - _Requirements: 1.1.5, 4.8.4_

  - [x] 7.8 Attach ECS Exec IAM policy to every task role
    - For each task role created by the IAM layer (edge-api, edge-poll, runtime, projection, controller, autoscaler, admin, mimir, loki, grafana), inline the ECS Exec policy: `ssmmessages:CreateControlChannel`, `ssmmessages:CreateDataChannel`, `ssmmessages:OpenControlChannel`, `ssmmessages:OpenDataChannel` (all with `Resource = "*"`)
    - The policy lives on the task role, NOT the execution role. This is non-obvious and easy to misplace
    - _Requirements: 3.4.5_

  - [x]* 7.9 Write unit tests for services module and deploy-engine integration
    - Test `ServicesModule` returns correct name and dependencies (`observability` — not `cluster` directly)
    - Test all 7 services are generated with correct scheduling types
    - Test service dependencies form a DAG
    - Test manifest generation produces stable JSON for unchanged config
    - Test every service has `enable_execute_command = true` and primary `initProcessEnabled = true`
    - Test every task role has the ECS Exec inline policy (4 ssmmessages actions only)
    - Test every service registers a `metrics` Service Connect alias at port 9090 with discovery name `<service>-metrics`
    - Test edge-api's task definition includes two wait-for init containers (`wait-for-tokeira-runtime`, `wait-for-tokeira-controller`), each essential=false with SUCCESS dependency from the primary
    - Test grafana's task definition includes `wait-for-tokeira-mimir` and `wait-for-tokeira-loki`
    - Test `validate_cpu_memory` accepts (1024, 2048) and rejects (3584, 6656) with a helpful error
    - **Property 4: Service DAG** — verify service dependency graph is acyclic
    - **Property 9: Manifest stability** — generate manifests twice from same config, assert identical
    - _Requirements: 3.4, 4.8, 4.9, 5.1, 5.2, 7.3, 7.4_

- [ ] 8. Checkpoint — Phase 4 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [ ] 8.5 Phase 4b — Observability Stack
  - [x] 8.5.1 Add `ObservabilityStackConfig` to `EcsConfig`
    - Add required `observability: ObservabilityStackConfig` section to `EcsConfig`
    - Define fields: Mimir/Loki/Grafana/Alloy image versions, CPU/memory limits, S3 bucket names, `retention_days`
    - Default values match compose platform pinned versions: Mimir 3.0.6, Loki 3.7.1, Grafana 12.4.3, Alloy v1.16.0
    - `retention_days` defaults to 30; applies to both Mimir metrics and Loki logs
    - _Requirements: 8.6.1, 8.6.2, 8.6.3, 8.6.4_

  - [x] 8.5.2 Implement Alloy metrics and log sidecar
    - Create `alloy_containers(service_name, project, metrics_port, config) -> (InitContainerSpec, ContainerDefinition)` returning (a) an `alloy-config-init` init container and (b) the Alloy metrics sidecar. See design §4b for the pattern
    - `alloy-config-init`: `amazon/aws-cli:latest` image, `essential = false`, mounts the `alloy-config` shared volume at `/etc/alloy`, fetches the per-service config from SSM, resolves the current ECS task ARN from `${ECS_CONTAINER_METADATA_URI_V4}/task`, derives `TASK_ID`, substitutes `TASK_ARN_PLACEHOLDER` and `TASK_ID_PLACEHOLDER`, and writes `/etc/alloy/config.alloy`
    - Alloy sidecar: `grafana/alloy:v1.16.0`, `essential = false`, `depends_on = [{container: alloy-config-init, condition: SUCCESS}]`, mounts `alloy-config` read-only at `/etc/alloy` and the host Docker socket read-only at `/var/run/docker.sock`, command `run /etc/alloy/config.alloy`, CPU/memory per config (default 128/256 for edge+runtime, 64/128 for control plane)
    - Configure each primary application container with no `logConfiguration` in the normal task definition so stdout/stderr remains in Docker json-file logs for Alloy to read
    - Task definition declares `volume { name: "alloy-config" }` as scratch storage and `volume { name: "docker-sock", host.source_path = "/var/run/docker.sock" }` for the read-only Docker socket mount
    - Add `AlloyParameterResource` implementing `Resource` trait. On create/update, call `ssm:PutParameter` at `/{project}/alloy/sidecar/{service_name}` with the rendered HCL config. On delete, call `ssm:DeleteParameter`
    - Render Alloy metrics/logs config from Askama template `alloy-sidecar-config.alloy.j2` with context: `service_name`, `project`, `environment`, `service_connect_namespace`, `metrics_port`, `mimir_endpoint = http://mimir.<namespace>:9009`, `loki_endpoint = http://loki.<namespace>:3100/loki/api/v1/push`, `TASK_ARN_PLACEHOLDER`, and `TASK_ID_PLACEHOLDER`
    - Enumerate one `AlloyParameterResource` per service (edge-api, edge-poll, runtime, projection, controller, autoscaler, admin, mimir, loki, grafana) from the `observability` module
    - Extend each service task role's IAM policy to allow `ssm:GetParameter` on `arn:aws:ssm:{region}:{account}:parameter/{project}/alloy/sidecar/*` so `alloy-config-init` can fetch its config at runtime. Do not grant service task roles write access to these parameters.
    - Wire `alloy_containers(...)` into all 10 task definitions in `ServicesModule` + `ObservabilityModule`
    - _Requirements: 8.1.1, 8.1.2, 8.1.3, 8.1.4, 8.1.5, 8.1.6, 8.1.7, 8.1.8, 8.1.9, 8.1.10_

  - [x] 8.5.3 Implement `ObservabilityModule`
    - Define `ObservabilityModule` implementing `Module` trait
    - `name()` returns `"observability"`, `dependencies()` returns `&["cluster"]`
    - `resources()` enumerates: S3 buckets for Mimir and Loki storage, IAM roles for S3 access, 10 `AlloyParameterResource` instances (one per service), Grafana admin `SecretsManagerResource`, Mimir/Loki/Grafana task definitions and ECS services
    - Add `SecretsManagerResource` implementing `Resource` trait. `create()` generates a random 32-char password via `secretsmanager:GenerateRandomPassword` and calls `secretsmanager:CreateSecret` with JSON `{"username":"admin","password":"<generated>"}` at name `{project_name}/grafana/admin`. `update()` is a no-op on secret value (operators rotate out-of-band). `delete()` calls `secretsmanager:DeleteSecret` with `RecoveryWindowInDays = 7` so accidental destroy can be recovered
    - Grafana task role grants `secretsmanager:GetSecretValue` on the secret ARN
    - Grafana task definition references the secret via `containerDefinitions.secrets` so ECS injects the password as `GRAFANA_ADMIN_PASSWORD` at task start
    - Mimir and Loki configured in single-binary mode with S3 backend
    - Grafana pre-configured with Mimir and Loki data sources
    - Each observability service runs on its own dedicated capacity provider: Mimir on `cp-mimir`, Loki on `cp-loki`, Grafana on `cp-grafana` (all with Service Connect)
    - Grafana is NOT registered with the ALB — reachable via `tkr port-forward grafana`
    - _Requirements: 8.1.1, 8.1.2, 8.1.3, 8.1.4, 8.1.5, 8.1.6, 8.1.7, 8.1.8, 8.1.9, 8.1.10, 8.2.1, 8.2.2, 8.2.3, 8.2.4, 8.2.5, 8.3.1, 8.3.2, 8.3.3, 8.3.4, 8.3.5, 8.4.1, 8.4.2, 8.4.3, 8.4.4, 8.4.5, 8.4.6, 8.4.7, 8.5.1, 8.5.2, 8.5.3, 8.5.4_

  - [x] 8.5.4 Wire `ObservabilityModule` into `EcsDeployment::infra_modules()`
    - Add `ObservabilityModule` between `ClusterModule` and `ServicesModule`; observability is mandatory for ECS deployments
    - `ObservabilityModule::dependencies()` returns `&["cluster"]`; `ServicesModule::dependencies()` returns `&["observability"]`
    - Final module dependency chain: `remote-state → networking → dsql → cluster → observability → services`
    - _Requirements: 7.4.1_

  - [x]* 8.5.5 Write unit tests for observability module
    - Test `ObservabilityModule` returns correct name and dependencies
    - Test resource enumeration includes S3 buckets, IAM roles, and 3 ECS services
    - Test Alloy sidecar container has the `alloy-config` mount, read-only Docker socket mount, and no primary-container `logConfiguration`
    - Test ECS config validation rejects a missing observability section
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

- [ ] 8.6 Checkpoint — Phase 4b tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [ ] 9. Phase 5 — Autoscaler Service
  - [x] 9.1 Create `crates/tokeira-autoscaler/` crate scaffold
    - Create `crates/tokeira-autoscaler/Cargo.toml` with dependencies: `tokeira-types`, `tokeira-config`, `tokeira-storage`, `tokeira-proto`, `aws-sdk-ecs`, `aws-sdk-autoscaling`, `reqwest`, `tokio`, `tonic`, `tracing`, `anyhow`, `serde`, `time`
    - Create module structure: `lib.rs`, `config.rs`, `leader.rs`, `mimir.rs`, `actuator.rs`, `loop_a.rs`, `loop_b.rs`, `loop_c.rs`, `envelope.rs`, `freshness.rs`, `reconciler.rs`
    - Add to workspace `Cargo.toml` members
    - _Requirements: 6.1.1, 6.1.2, 6.1.3_

  - [x] 9.2 Create `apps/tokeira-autoscaler/` binary entry point
    - Create `apps/tokeira-autoscaler/Cargo.toml` with dependencies: `tokeira-autoscaler`, `tokeira-config`, `anyhow`, `tokio`, `tracing-subscriber`
    - Create `apps/tokeira-autoscaler/src/main.rs` with config loading, leader election loop, and scaling loop orchestration
    - Add to workspace `Cargo.toml` members
    - _Requirements: 6.1.2_

  - [x] 9.3 Implement autoscaler configuration in `config.rs`
    - Define `AutoscalerServiceConfig` with fields: `polling_interval`, `scale_out_consecutive_samples`, `scale_in_consecutive_samples`, `cooldown`, `mimir_endpoint`, `staleness_threshold`, `dsql_connection_budget`, `dsql_connection_rate_budget`, `per_runtime_reserved_connections`, `per_runtime_startup_connection_rate`, `cluster_name`, `service_configs` (per-service min/max/step)
    - Use `serde(deny_unknown_fields)` and sensible defaults
    - _Requirements: 6.3.4, 6.3.5, 6.4.2, 6.7.3, 6.9.1, 6.9.2, 6.9.3, 6.9.4_

  - [x] 9.4 Implement DSQL leader lease in `leader.rs`
    - Implement `AutoscalerLeader` using `LeaseRepository::try_acquire_bundle` with a dedicated lease bundle
    - Implement `try_acquire()`, `renew()`, `is_leader()` methods
    - On renewal failure: clear epoch, stop writing decisions
    - Use a separate lease bundle ID from the controller's leader lease
    - _Requirements: 6.2.1, 6.2.2, 6.2.3, 6.2.4_

  - [x] 9.5 Implement Mimir client in `mimir.rs`
    - Implement `MimirClient` with `query_instant()` and `query_range()` methods
    - Query Mimir's Prometheus-compatible HTTP API (`/api/v1/query`, `/api/v1/query_range`)
    - Return `MetricFreshness` enum: `Fresh`, `Stale`, `Missing`
    - Implement `is_available()` health check
    - _Requirements: 6.3.1, 6.4.1, 6.6.1_

  - [x] 9.6 Implement AWS actuator in `actuator.rs`
    - Implement `AwsActuator` wrapping `aws_sdk_ecs::Client` and `aws_sdk_autoscaling::Client`
    - `update_service_desired_count()`: no-op if already at target, returns whether change was made
    - `set_asg_desired_capacity()`: no-op if already at target
    - `drain_container_instance()`: set instance to DRAINING via `ecs:UpdateContainerInstancesState`
    - `clear_instance_protection()`: remove scale-in protection via `autoscaling:SetInstanceProtection`
    - `terminate_instance_with_decrement()`: call `autoscaling:TerminateInstanceInAutoScalingGroup` with `ShouldDecrementDesiredCapacity=true`
    - `describe_service()` and `describe_asg()`: return current state
    - `resolve_container_instance_for_ec2(ec2_id) -> container_instance_arn`: uses `ecs:ListContainerInstances` + `ecs:DescribeContainerInstances` so Loop C can convert the instance chosen for retirement into the ECS control identifier
    - Implement exponential backoff on throttling errors
    - The full IAM surface required on the autoscaler task role is documented in Req 4.6.3; this task drives the permission list
    - _Requirements: 4.6.3, 6.3.2, 6.3.3, 6.4.3, 6.4.4, 6.5.4, 6.5.5, 6.5.6, 6.8.2, 6.8.3_

  - [x] 9.7 Implement connection-aware scaling envelope in `envelope.rs`
    - Implement `ScalingEnvelope` with `effective_max_runtime_hosts()` and `allows_scale_to()` methods
    - `effective_max_runtime_hosts = min(configured_max, floor(budget/per_runtime), floor(rate_budget/per_runtime_rate))`
    - _Requirements: 6.7.1, 6.7.2, 6.7.3_

  - [x] 9.8 Implement metric freshness tracker in `freshness.rs`
    - Implement `FreshnessTracker` with `scaling_permission()` method
    - Missing data is not zero — absent metrics block scale-in
    - Stale metrics block scale-in for that plane
    - Stale controller snapshot blocks runtime scale-in
    - Unknown DSQL headroom constrains runtime scale-out to floor
    - Mimir unavailable freezes desired capacity
    - Overload signals allow scale-out even with partial metrics
    - _Requirements: 6.6.1, 6.6.2, 6.6.3, 6.6.4, 6.6.5, 6.6.6_

  - [x] 9.9 Implement desired-state reconciler in `reconciler.rs`
    - Implement `DesiredState` with `service_counts`, `asg_capacities`, `drain_intents`
    - Implement `reconcile()` that compares desired vs current and returns `Vec<ScalingAction>`
    - No-op when desired matches current (idempotent)
    - Track drain intents through phases: `ControllerDraining → EcsDraining → ProtectionCleared → Terminated`
    - Record every scaling decision with input metrics and reason
    - _Requirements: 6.8.1, 6.8.2, 6.8.4_

  - [x] 9.10 Implement Loop A — REPLICA service scaling in `loop_a.rs`
    - Query Mimir for per-service scaling signals
    - Compute target desired count based on signals and config (min/max/step)
    - Apply hysteresis: require `scale_out_consecutive_samples` for scale-out, `scale_in_consecutive_samples` for scale-in
    - Update desired state in reconciler
    - _Requirements: 6.3.1, 6.3.2, 6.3.3, 6.3.4, 6.3.5, 6.9.1, 6.9.2, 6.9.3, 6.9.4_

  - [x] 9.11 Implement Loop B — Runtime scale-out in `loop_b.rs`
    - Query Mimir for runtime pressure signals
    - Classify pressure: broad saturation, hot-node imbalance, hot-bundle imbalance, DSQL-bound, admission-bound
    - Only scale out on broad saturation with sufficient DSQL headroom
    - Verify connection envelope allows the target host count
    - Update desired state in reconciler
    - _Requirements: 6.4.1, 6.4.2, 6.4.3, 6.4.4, 6.4.5, 6.4.6_

  - [x] 9.12 Implement Loop C — Runtime retirement in `loop_c.rs`
    - Determine excess capacity from Mimir metrics and controller snapshot
    - Request candidates from controller via `NominateScaleInCandidates` gRPC
    - Mark candidates as draining via `MarkNodeDraining` gRPC
    - Monitor drain progress via controller
    - When safe-to-terminate: drain ECS instance → clear protection → terminate with decrement
    - Never reduce ASG desired capacity directly
    - _Requirements: 6.5.1, 6.5.2, 6.5.3, 6.5.4, 6.5.5, 6.5.6, 6.5.7_

  - [x]* 9.13 Write property tests for autoscaler components
    - **Property 5: Envelope monotonicity** — effective_max decreases as per_runtime_reserved increases
    - **Property 6: Envelope correctness** — allows_scale_to consistent with effective_max
    - **Property 7: Reconciliation idempotency** — matching desired/current produces empty actions
    - **Property 8: Freshness safety** — Mimir unavailable never allows scale-in
    - Use `proptest` crate
    - _Validates: Requirements 6.6, 6.7, 6.8_

  - [ ]* 9.14 Write unit tests for autoscaler components
    - Test leader lease: acquire, renew, revert on failure
    - Test Mimir client: parse instant query response, handle missing series, detect staleness
    - Test AWS actuator: no-op when state matches target (mocked clients)
    - Test scaling envelope: known-input computation, edge cases (zero budget)
    - Test freshness tracker: all cells of the decision matrix
    - Test reconciler: no-op for matching state, correct actions for differing state
    - Test Loop A: correct desired count computation with hysteresis
    - Test Loop B: scale-out blocked when DSQL headroom insufficient, blocked for hot-bundle imbalance
    - Test Loop C: correct drain phase progression
    - _Requirements: 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9_

- [ ] 10. Checkpoint — Phase 5 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [ ] 11. Phase 6 — CLI Integration
  - [x] 11.1 Wire ECS platform into `tkr` CLI
    - Add `PlatformKind::Ecs` handling in `tkr/src/commands/infra.rs` and `tkr/src/commands/deploy.rs`
    - Load `EcsConfig` from deployment TOML when ECS platform is selected
    - Wire `tkr infra plan`, `tkr infra apply`, `tkr infra destroy` to `EcsDeployment`
    - Wire `tkr deploy plan`, `tkr deploy apply` to `EcsDeployment`
    - _Requirements: 9.1.1, 9.1.2, 9.1.3, 9.1.4_

  - [x] 11.2 Add prototypical config generation for ECS
    - Add ECS case to `tkr/src/prototypical.rs` (or equivalent)
    - `tkr init --platform ecs` generates `deployment.toml` with ECS defaults and `tokeirad.toml` with DSQL config
    - The generated `[dsql]` section SHALL include `mode = "managed"` with placeholder `endpoint`/`*_id`/`*_arn` fields commented out, plus an example `[dsql]` block with `mode = "preexisting"` and all required fields filled in as comments for operators adopting an existing DSQL cluster
    - _Requirements: 9.2.1, 9.2.2, 9.2.3_

  - [ ] 11.3 Implement ECS operations commands
    - Implement `scale_up` via `ecs:UpdateService` (increase desired count)
    - Implement `scale_down` via `ecs:UpdateService` (decrease desired count)
    - Implement `logs` via ECS task log retrieval
    - Validate service names against `valid_services()`
    - _Requirements: 9.3.1, 9.3.2, 9.3.3_

  - [ ] 11.4 Implement `tkr port-forward` for ECS services
    - Add a `port-forward` subcommand to `tkr/src/cli.rs` accepting `<service>` and optional `--local-port`
    - Implement `commands::port_forward::run_ecs(service, local_port, config)` in `tkr/src/commands/port_forward.rs`
    - Service-to-default-port mapping: `grafana=3000`, `edge-api=7233`, `edge-poll=7234`, `controller=7240`, `mimir=9009`, `loki=3100`
    - Discover a running container instance: `ecs:ListContainerInstances` filtered by the service's capacity provider, then `ecs:DescribeContainerInstances` to get the `ec2InstanceId`
    - For services reachable on the container instance itself (Grafana, Mimir, Loki — each runs as a single task per host), use `AWS-StartPortForwardingSession` targeting the instance ID with `portNumber` set to the container port
    - For services on replica pools (edge-api, edge-poll, controller), use `AWS-StartPortForwardingSessionToRemoteHost` with `host` set to the Service Connect endpoint (e.g., `controller.tokeira.local`)
    - Shell out to `aws ssm start-session` rather than embedding the SSM data-plane protocol (matches how dsqld-cli handles EKS port-forward)
    - Require `session-manager-plugin` on the operator's machine; print an install hint when missing
    - _Requirements: 9.6.1, 9.6.2, 9.6.3, 9.6.4, 9.6.6, 9.6.7_

  - [x] 11.5 Add SSM managed policy to instance profiles
    - Attach `AmazonSSMManagedInstanceCore` to every Auto Scaling group's IAM instance profile created by `ClusterModule`
    - Verify SSM VPC endpoints (`ssm`, `ssmmessages`, `ec2messages`) are present in the required endpoint set so Session Manager works without internet
    - _Requirements: 9.6.5, 3.3.2, 3.4.6_

  - [ ] 11.6 Implement `tkr exec` for interactive container access
    - Add an `exec` subcommand to `tkr/src/cli.rs` accepting `<service> [--container <name>] -- <cmd>...`
    - Implement `commands::exec::run_ecs(service, container, cmd, config)` in `tkr/src/commands/exec.rs`
    - Resolve the container name: if `--container` omitted, default to the primary application container for the service (`tokeira-runtime` for runtime, `tokeira-mimir` for mimir, etc.). Never default to the Alloy sidecar
    - Find a running task: `ecs:ListTasks --cluster <cluster> --service-name <service> --desired-status RUNNING`, then pick the first ARN
    - Call `ecs:ExecuteCommand` with `interactive = true`, `container`, `task`, and `command`
    - Hand off the returned session payload to `session-manager-plugin` (same approach as port-forward)
    - Require `session-manager-plugin` on the operator's machine; print an install hint when missing
    - Print a helpful error with remediation hints if the task role is missing `ssmmessages:*` permissions (first-deploy misconfiguration)
    - _Requirements: 3.4.7, 3.4.8_

  - [ ] 11.7 Implement `tkr admin <subcommand>` for on-demand admin execution
    - Add an `admin` subcommand group to `tkr/src/cli.rs` that accepts any sub-subcommand (passed through to the admin binary)
    - Implement `commands::admin::run(subcommand, args, config)` that: scales `tokeira-admin` from 0 to 1, polls `ecs:DescribeServices` until `runningCount == 1` and `desiredCount == 1` with a 120s default timeout, calls `ecs:ExecuteCommand` against the running task with the supplied subcommand, streams output, and scales back to 0 in a `finally`-equivalent block (so Ctrl-C still scales down)
    - If the task fails to reach RUNNING within timeout, fetch `ecs:DescribeTasks` for the task's `stoppedReason` and surface it in the error message. Do NOT scale back to 0 on this failure path — the next `tkr admin` call decides whether to reuse the failing service or scale-to-0 first
    - Minimum subcommand surface: `tkr admin schema setup`, `tkr admin schema migrate <version>`, `tkr admin schema status`, `tkr admin diagnostics <target>`. Forward the full subcommand-plus-args to the admin binary via `--command`
    - _Requirements: 9.7.1, 9.7.2, 9.7.3, 9.7.4_

  - [ ]* 11.8 Write CLI integration tests
    - Test `tkr init --platform ecs` generates valid TOML
    - Test CLI parse for ECS-specific commands
    - Test invalid service name produces helpful error
    - Test `tkr port-forward` command parsing
    - Test `tkr exec` command parsing (default container resolution, `--container` override)
    - Test `tkr admin schema setup` command parsing
    - _Requirements: 9.1, 9.2, 9.3, 9.6, 9.7, 3.4.7, 3.4.8_

- [ ] 12. Final checkpoint — All tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.
  - Run `cargo lint` and `cargo +nightly fmt --all --check` to verify code quality.
  - Run `cargo doc --workspace --no-deps` to verify documentation builds.

## Notes

- Tasks marked with `*` are optional property/unit test tasks that can be deferred for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation per phase
- The `platforms/ecs/` crate follows the same structure as `platforms/compose/`: `config.rs`, `modules.rs`, `services.rs`, `lib.rs`
- The `tokeira-autoscaler` crate is a library; the binary entry point is in `apps/tokeira-autoscaler/`
- AWS resource implementations in `tokeira-aws` are extended with new resource types (ECS cluster, ASG, capacity provider, launch template, ALB, task definition, ECS service, Cloud Map namespace)
- The autoscaler uses the existing `LeaseRepository` trait for leader election, avoiding a new storage abstraction
- The autoscaler communicates with the controller via the gRPC service defined in the `shard-placement-membership` spec
- For the first iteration: no CloudWatch metrics in the decision loop, no ECS managed cluster auto scaling for runtime, no Application Auto Scaling target tracking
- The `tokeira-runtime` DAEMON scheduling is a placement profile, not a correctness dependency — the lease and placement model works regardless of scheduling strategy
