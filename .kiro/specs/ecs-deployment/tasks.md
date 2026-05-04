# Implementation Plan: ECS Deployment

## Overview

Implement the ECS on EC2 deployment infrastructure for Tokeira. The work is organized into 6 phases: platform crate scaffold, networking module, cluster module, services module, autoscaler service, and CLI integration. Each phase builds on the previous one. Two new crates are introduced: `platforms/ecs/` (platform) and `crates/tokeira-autoscaler/` (autoscaler library). A new binary entry point is added at `apps/tokeira-autoscaler/`.

## Tasks

- [ ] 1. Phase 1 — ECS Platform Crate and Configuration
  - [ ] 1.1 Create `platforms/ecs/` crate scaffold
    - Create `platforms/ecs/Cargo.toml` with dependencies: `tokeira-iac`, `tokeira-deploy-engine`, `tokeira-orchestrator`, `tokeira-config`, `tokeira-aws`, `tokeira-state`, `tokeira-types`, `serde`, `async-trait`, `tracing`, `anyhow`
    - Create `platforms/ecs/src/lib.rs` with module declarations: `config`, `modules`, `services`
    - Add `platforms/ecs` to workspace `Cargo.toml` members
    - _Requirements: 1.2.1_

  - [ ] 1.2 Implement ECS configuration model in `platforms/ecs/src/config.rs`
    - Define `EcsConfig` with sections: `ClusterConfig`, `NetworkingConfig`, `CapacityProviderConfigs`, `ServiceConfigs`, `AutoscalerConfig`, `AlbConfig`
    - Define `CapacityProviderConfig` and `RuntimeCapacityProviderConfig` (with `scale_in_protection` field)
    - Define `ReplicaServiceConfig` and `DaemonServiceConfig`
    - Define `OptionalEndpoints` struct for optional VPC endpoints
    - Use `serde(deny_unknown_fields)` on all config structs
    - Implement `Default` for `EcsConfig` with sensible defaults for a single-environment private-only deployment
    - Implement `validate()` method that checks for invalid combinations (empty subnet list, zero capacity, etc.)
    - _Requirements: 1.1.1, 1.1.2, 1.1.3, 1.1.4, 1.1.5_

  - [ ] 1.3 Add `PlatformKind::Ecs` variant to `tokeira-orchestrator`
    - Add `Ecs` variant to `PlatformKind` enum in `tokeira-orchestrator/src/lib.rs`
    - Add `Ecs` variant to `CliPlatformKind` in `tkr/src/cli.rs` (if separate)
    - _Requirements: 1.2.5, 8.1.1_

  - [ ] 1.4 Implement `PlatformConfig` trait for `EcsDeployment`
    - Implement `prototypical_config()` returning default `EcsConfig` as TOML
    - Implement `prototypical_server_config()` returning `TokeiraConfig` with DSQL endpoint placeholder
    - _Requirements: 1.2.4, 8.2.1, 8.2.2, 8.2.3_

  - [ ] 1.5 Implement stub `Deployment` and `Ops` traits for `EcsDeployment`
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
    - Test `valid_services()` returns all 10 service names (7 Tokeira + 3 observability)
    - Test invalid service name produces error listing valid alternatives
    - **Property 10: Invalid service rejection** — generate random strings not in valid set, verify error
    - _Requirements: 1.1.4, 8.3.3_

- [ ] 2. Checkpoint — Phase 1 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.
  - Run `cargo lint` to verify compilation.

- [ ] 3. Phase 2 — Networking IaC Module
  - [ ] 3.1 Implement security group resources in `platforms/ecs/src/modules.rs`
    - Define security group resources for: ALB, edge services, runtime services, control services, VPC endpoints
    - Each security group resource implements the `Resource` trait using `tokeira-aws` `SecurityGroupResource`
    - Security groups allow only minimum required ingress/egress per component
    - No `0.0.0.0/0` ingress rules
    - _Requirements: 2.4.1, 2.4.2, 2.4.3_

  - [ ] 3.2 Implement VPC endpoint resources
    - Define a helper function `required_vpc_endpoints(region)` returning the 10 required endpoints: ECS(3), ECR(2), S3(gw), AutoScaling, CloudMap, DSQL(2)
    - Define a helper function `optional_vpc_endpoints(config, region)` returning enabled optional endpoints
    - Each VPC endpoint resource implements the `Resource` trait using `tokeira-aws` `VpcEndpointResource`
    - _Requirements: 2.2.1, 2.2.2, 2.2.3, 2.2.4, 2.2.5, 2.2.6, 2.2.7_

  - [ ] 3.3 Implement internal ALB resources
    - Define ALB resource, target group resources (edge-api, edge-poll), and listener resource
    - ALB is internal-only, placed in private subnets
    - Target groups support split private DNS names: `edge-api.<zone>` and `edge-poll.<zone>`
    - Implement as `Resource` trait implementations in `tokeira-aws` (new resource types: `AlbResource`, `AlbTargetGroupResource`, `AlbListenerResource`)
    - _Requirements: 2.3.1, 2.3.2, 2.3.3, 2.3.4_

  - [ ] 3.4 Implement `NetworkingModule`
    - Define `NetworkingModule` implementing `Module` trait
    - `name()` returns `"networking"`, `dependencies()` returns `&["remote-state"]`
    - `resources()` enumerates: security groups, VPC endpoints, ALB resources
    - _Requirements: 7.1.1, 7.1.2, 7.1.3, 7.1.4_

  - [ ]* 3.5 Write unit tests for networking module
    - Test `NetworkingModule` returns correct name and dependencies
    - Test resource enumeration includes all required VPC endpoints
    - Test optional endpoints are included only when enabled in config
    - Test security group resources report correct module name
    - _Requirements: 2.2, 2.4, 7.1_

- [ ] 4. Checkpoint — Phase 2 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [ ] 5. Phase 3 — Cluster IaC Module
  - [ ] 5.1 Implement new AWS resource types in `tokeira-aws`
    - Add `EcsClusterResource` implementing `Resource` trait (create/describe/delete ECS cluster)
    - Add `LaunchTemplateResource` implementing `Resource` trait (ECS-optimized AMI, user data for ECS agent)
    - Add `AsgResource` implementing `Resource` trait (create/update/delete ASG with instance protection config)
    - Add `CapacityProviderResource` implementing `Resource` trait (create/delete capacity provider linked to ASG)
    - Add `IamInstanceProfileResource` implementing `Resource` trait (instance profile for ECS agent)
    - _Requirements: 3.2.1, 3.2.2, 3.2.3, 3.2.4, 3.3.1, 3.3.2, 3.3.3_

  - [ ] 5.2 Implement `ClusterModule`
    - Define `ClusterModule` implementing `Module` trait
    - `name()` returns `"cluster"`, `dependencies()` returns `&["networking"]`
    - `resources()` enumerates: ECS cluster, 5 IAM instance profiles, 5 launch templates, 5 ASGs, 5 capacity providers
    - Runtime ASG has `NewInstancesProtectedFromScaleIn: true`
    - _Requirements: 3.1.1, 3.1.2, 3.1.3, 3.2.1, 3.2.2, 3.2.3, 3.2.4, 3.2.5, 7.2.1, 7.2.2, 7.2.3, 7.2.4_

  - [ ]* 5.3 Write unit tests for cluster module
    - Test `ClusterModule` returns correct name and dependencies
    - Test resource enumeration includes ECS cluster, 5 CPs, 5 ASGs
    - Test runtime ASG has scale-in protection enabled
    - Test all resources report correct module name
    - **Property 3: Module DAG** — verify module dependency graph is acyclic
    - _Requirements: 3.1, 3.2, 3.3, 7.2, 7.4_

- [ ] 6. Checkpoint — Phase 3 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [ ] 7. Phase 4 — Services IaC Module and Deploy-Engine Integration
  - [ ] 7.1 Implement ECS service and task definition resource types in `tokeira-aws`
    - Add `TaskDefinitionResource` implementing `Resource` trait (register/deregister task definition)
    - Add `EcsServiceResource` implementing `Resource` trait (create/update/delete ECS service)
    - Add `CloudMapNamespaceResource` implementing `Resource` trait (create/delete private DNS namespace)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 5.2.1_

  - [ ] 7.2 Implement `ServicesModule`
    - Define `ServicesModule` implementing `Module` trait
    - `name()` returns `"services"`, `dependencies()` returns `&["cluster"]`
    - `resources()` enumerates: Cloud Map namespace, 7 task definitions (each with Alloy sidecar container), 7 ECS services
    - Each task definition includes the primary application container + Alloy sidecar container
    - Alloy sidecar configured with `MIMIR_REMOTE_WRITE_URL`, `LOKI_WRITE_URL`, `METRICS_SCRAPE_TARGET` environment variables
    - Service Connect configuration on applicable services
    - Runtime service registers in Cloud Map namespace
    - _Requirements: 5.1.1, 5.1.2, 5.1.3, 5.2.1, 5.2.2, 5.2.3, 7.3.1, 7.3.2, 7.3.3, 7.3.4, 8.1.1, 8.1.2, 8.1.3, 8.1.4, 8.1.5, 8.1.6, 8.1.7_

  - [ ] 7.3 Implement `EcsWorkload` deploy-engine service adapter
    - Define `EcsWorkload` struct with `name`, `scheduling` (REPLICA/DAEMON), `capacity_provider`, `task_definition`
    - Implement `deploy_engine::Service` trait: `name()`, `module()`, `dependencies()`, `manifests()`
    - Define service dependencies: edge depends on runtime+controller, projection depends on runtime, autoscaler depends on controller
    - Generate task definition JSON as manifest
    - _Requirements: 4.8.1, 4.8.2, 4.8.3_

  - [ ] 7.4 Implement `EcsImage` deploy-engine image adapter
    - Define `EcsImage` struct wrapping ECR image references
    - Implement `deploy_engine::Image` trait
    - _Requirements: 4.1.2, 4.2.2, 4.3.2, 4.4.2, 4.5.2, 4.6.2, 4.7.2_

  - [ ] 7.5 Wire services into `EcsDeployment::services()` and `EcsDeployment::images()`
    - Generate all 7 `EcsWorkload` instances from `EcsConfig`
    - Generate all `EcsImage` instances from `EcsConfig`
    - Verify scheduling types: DAEMON for runtime, REPLICA for all others
    - Verify capacity provider assignments match the design
    - _Requirements: 4.1.1, 4.2.1, 4.3.1, 4.4.1, 4.5.1, 4.6.1, 4.7.1_

  - [ ] 7.6 Wire `infra_modules` to return all three modules
    - Update `EcsDeployment::infra_modules()` to return `NetworkingModule`, `ClusterModule`, `ServicesModule` filtered by `ModuleSelection`
    - Verify module dependency ordering: `remote-state → networking → cluster → services`
    - _Requirements: 7.4.1, 7.4.2, 7.4.3_

  - [ ]* 7.7 Write unit tests for services module and deploy-engine integration
    - Test `ServicesModule` returns correct name and dependencies
    - Test all 7 services are generated with correct scheduling types
    - Test service dependencies form a DAG
    - Test manifest generation produces stable JSON for unchanged config
    - **Property 4: Service DAG** — verify service dependency graph is acyclic
    - **Property 9: Manifest stability** — generate manifests twice from same config, assert identical
    - _Requirements: 4.8, 5.1, 5.2, 7.3, 7.4_

- [ ] 8. Checkpoint — Phase 4 tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [ ] 8.5 Phase 4b — Observability Stack
  - [ ] 8.5.1 Add `ObservabilityStackConfig` to `EcsConfig`
    - Add `observability: Option<ObservabilityStackConfig>` section to `EcsConfig`
    - Define fields: Mimir/Loki/Grafana/Alloy image versions, CPU/memory limits, S3 bucket names
    - Default values match compose platform pinned versions: Mimir 3.0.6, Loki 3.7.1, Grafana 12.4.3, Alloy v1.16.0
    - _Requirements: 8.6.1, 8.6.2, 8.6.3_

  - [ ] 8.5.2 Implement Alloy sidecar container helper
    - Create `alloy_sidecar_container(metrics_port, config)` function returning a container definition
    - Configure environment: `MIMIR_REMOTE_WRITE_URL`, `LOKI_WRITE_URL`, `METRICS_SCRAPE_TARGET`
    - Set resource limits from config (default: 64 CPU units, 128 MB memory)
    - Mark as `essential: false` so sidecar failure does not kill the primary container
    - Wire into all 7 Tokeira task definitions in `ServicesModule`
    - _Requirements: 8.1.1, 8.1.2, 8.1.3, 8.1.4, 8.1.5, 8.1.6, 8.1.7_

  - [ ] 8.5.3 Implement `ObservabilityModule`
    - Define `ObservabilityModule` implementing `Module` trait
    - `name()` returns `"observability"`, `dependencies()` returns `&["services"]`
    - `resources()` enumerates: S3 buckets for Mimir and Loki storage, IAM roles for S3 access, Mimir/Loki/Grafana task definitions and ECS services
    - Mimir and Loki configured in single-binary mode with S3 backend
    - Grafana pre-configured with Mimir and Loki data sources
    - All three services on `cp-control` capacity provider with Service Connect
    - _Requirements: 8.2.1, 8.2.2, 8.2.3, 8.2.4, 8.2.5, 8.3.1, 8.3.2, 8.3.3, 8.3.4, 8.3.5, 8.4.1, 8.4.2, 8.4.3, 8.4.4, 8.4.5, 8.5.1, 8.5.2, 8.5.3, 8.5.4_

  - [ ] 8.5.4 Wire `ObservabilityModule` into `EcsDeployment::infra_modules()`
    - Add `ObservabilityModule` after `ServicesModule` when `config.observability.is_some()`
    - Update module dependency chain: `remote-state → networking → cluster → services → observability`
    - _Requirements: 7.4.1_

  - [ ]* 8.5.5 Write unit tests for observability module
    - Test `ObservabilityModule` returns correct name and dependencies
    - Test resource enumeration includes S3 buckets, IAM roles, and 3 ECS services
    - Test Alloy sidecar container has correct environment variables
    - Test observability module is omitted when config section is `None`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

- [ ] 8.6 Checkpoint — Phase 4b tests pass
  - Run `cargo test --workspace` and verify all new and existing tests pass.

- [ ] 9. Phase 5 — Autoscaler Service
  - [ ] 9.1 Create `crates/tokeira-autoscaler/` crate scaffold
    - Create `crates/tokeira-autoscaler/Cargo.toml` with dependencies: `tokeira-types`, `tokeira-config`, `tokeira-storage`, `tokeira-proto`, `aws-sdk-ecs`, `aws-sdk-autoscaling`, `reqwest`, `tokio`, `tonic`, `tracing`, `anyhow`, `serde`, `time`
    - Create module structure: `lib.rs`, `config.rs`, `leader.rs`, `mimir.rs`, `actuator.rs`, `loop_a.rs`, `loop_b.rs`, `loop_c.rs`, `envelope.rs`, `freshness.rs`, `reconciler.rs`
    - Add to workspace `Cargo.toml` members
    - _Requirements: 6.1.1, 6.1.2, 6.1.3_

  - [ ] 9.2 Create `apps/tokeira-autoscaler/` binary entry point
    - Create `apps/tokeira-autoscaler/Cargo.toml` with dependencies: `tokeira-autoscaler`, `tokeira-config`, `anyhow`, `tokio`, `tracing-subscriber`
    - Create `apps/tokeira-autoscaler/src/main.rs` with config loading, leader election loop, and scaling loop orchestration
    - Add to workspace `Cargo.toml` members
    - _Requirements: 6.1.2_

  - [ ] 9.3 Implement autoscaler configuration in `config.rs`
    - Define `AutoscalerServiceConfig` with fields: `polling_interval`, `scale_out_consecutive_samples`, `scale_in_consecutive_samples`, `cooldown`, `mimir_endpoint`, `staleness_threshold`, `dsql_connection_budget`, `dsql_connection_rate_budget`, `per_runtime_reserved_connections`, `per_runtime_startup_connection_rate`, `cluster_name`, `service_configs` (per-service min/max/step)
    - Use `serde(deny_unknown_fields)` and sensible defaults
    - _Requirements: 6.3.4, 6.3.5, 6.4.2, 6.7.3, 6.9.1, 6.9.2, 6.9.3, 6.9.4_

  - [ ] 9.4 Implement DSQL leader lease in `leader.rs`
    - Implement `AutoscalerLeader` using `LeaseRepository::try_acquire_bundle` with a dedicated lease bundle
    - Implement `try_acquire()`, `renew()`, `is_leader()` methods
    - On renewal failure: clear epoch, stop writing decisions
    - Use a separate lease bundle ID from the controller's leader lease
    - _Requirements: 6.2.1, 6.2.2, 6.2.3, 6.2.4_

  - [ ] 9.5 Implement Mimir client in `mimir.rs`
    - Implement `MimirClient` with `query_instant()` and `query_range()` methods
    - Query Mimir's Prometheus-compatible HTTP API (`/api/v1/query`, `/api/v1/query_range`)
    - Return `MetricFreshness` enum: `Fresh`, `Stale`, `Missing`
    - Implement `is_available()` health check
    - _Requirements: 6.3.1, 6.4.1, 6.6.1_

  - [ ] 9.6 Implement AWS actuator in `actuator.rs`
    - Implement `AwsActuator` wrapping `aws_sdk_ecs::Client` and `aws_sdk_autoscaling::Client`
    - `update_service_desired_count()`: no-op if already at target, returns whether change was made
    - `set_asg_desired_capacity()`: no-op if already at target
    - `drain_container_instance()`: set instance to DRAINING
    - `clear_instance_protection()`: remove scale-in protection
    - `terminate_instance_with_decrement()`: call `TerminateInstanceInAutoScalingGroup` with `ShouldDecrementDesiredCapacity=true`
    - `describe_service()` and `describe_asg()`: return current state
    - Implement exponential backoff on throttling errors
    - _Requirements: 6.3.2, 6.3.3, 6.4.3, 6.4.4, 6.5.4, 6.5.5, 6.5.6, 6.8.2, 6.8.3_

  - [ ] 9.7 Implement connection-aware scaling envelope in `envelope.rs`
    - Implement `ScalingEnvelope` with `effective_max_runtime_hosts()` and `allows_scale_to()` methods
    - `effective_max_runtime_hosts = min(configured_max, floor(budget/per_runtime), floor(rate_budget/per_runtime_rate))`
    - _Requirements: 6.7.1, 6.7.2, 6.7.3_

  - [ ] 9.8 Implement metric freshness tracker in `freshness.rs`
    - Implement `FreshnessTracker` with `scaling_permission()` method
    - Missing data is not zero — absent metrics block scale-in
    - Stale metrics block scale-in for that plane
    - Stale controller snapshot blocks runtime scale-in
    - Unknown DSQL headroom constrains runtime scale-out to floor
    - Mimir unavailable freezes desired capacity
    - Overload signals allow scale-out even with partial metrics
    - _Requirements: 6.6.1, 6.6.2, 6.6.3, 6.6.4, 6.6.5, 6.6.6_

  - [ ] 9.9 Implement desired-state reconciler in `reconciler.rs`
    - Implement `DesiredState` with `service_counts`, `asg_capacities`, `drain_intents`
    - Implement `reconcile()` that compares desired vs current and returns `Vec<ScalingAction>`
    - No-op when desired matches current (idempotent)
    - Track drain intents through phases: `ControllerDraining → EcsDraining → ProtectionCleared → Terminated`
    - Record every scaling decision with input metrics and reason
    - _Requirements: 6.8.1, 6.8.2, 6.8.4_

  - [ ] 9.10 Implement Loop A — REPLICA service scaling in `loop_a.rs`
    - Query Mimir for per-service scaling signals
    - Compute target desired count based on signals and config (min/max/step)
    - Apply hysteresis: require `scale_out_consecutive_samples` for scale-out, `scale_in_consecutive_samples` for scale-in
    - Update desired state in reconciler
    - _Requirements: 6.3.1, 6.3.2, 6.3.3, 6.3.4, 6.3.5, 6.9.1, 6.9.2, 6.9.3, 6.9.4_

  - [ ] 9.11 Implement Loop B — Runtime scale-out in `loop_b.rs`
    - Query Mimir for runtime pressure signals
    - Classify pressure: broad saturation, hot-node imbalance, hot-bundle imbalance, DSQL-bound, admission-bound
    - Only scale out on broad saturation with sufficient DSQL headroom
    - Verify connection envelope allows the target host count
    - Update desired state in reconciler
    - _Requirements: 6.4.1, 6.4.2, 6.4.3, 6.4.4, 6.4.5, 6.4.6_

  - [ ] 9.12 Implement Loop C — Runtime retirement in `loop_c.rs`
    - Determine excess capacity from Mimir metrics and controller snapshot
    - Request candidates from controller via `NominateScaleInCandidates` gRPC
    - Mark candidates as draining via `MarkNodeDraining` gRPC
    - Monitor drain progress via controller
    - When safe-to-terminate: drain ECS instance → clear protection → terminate with decrement
    - Never reduce ASG desired capacity directly
    - _Requirements: 6.5.1, 6.5.2, 6.5.3, 6.5.4, 6.5.5, 6.5.6, 6.5.7_

  - [ ]* 9.13 Write property tests for autoscaler components
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
  - [ ] 11.1 Wire ECS platform into `tkr` CLI
    - Add `PlatformKind::Ecs` handling in `tkr/src/commands/infra.rs` and `tkr/src/commands/deploy.rs`
    - Load `EcsConfig` from deployment TOML when ECS platform is selected
    - Wire `tkr infra plan`, `tkr infra apply`, `tkr infra destroy` to `EcsDeployment`
    - Wire `tkr deploy plan`, `tkr deploy apply` to `EcsDeployment`
    - _Requirements: 8.1.1, 8.1.2, 8.1.3, 8.1.4_

  - [ ] 11.2 Add prototypical config generation for ECS
    - Add ECS case to `tkr/src/prototypical.rs` (or equivalent)
    - `tkr init --platform ecs` generates `deployment.toml` with ECS defaults and `tokeirad.toml` with DSQL config
    - _Requirements: 8.2.1, 8.2.2, 8.2.3_

  - [ ] 11.3 Implement ECS operations commands
    - Implement `scale_up` via `ecs:UpdateService` (increase desired count)
    - Implement `scale_down` via `ecs:UpdateService` (decrease desired count)
    - Implement `logs` via ECS task log retrieval
    - Validate service names against `valid_services()`
    - _Requirements: 8.3.1, 8.3.2, 8.3.3_

  - [ ]* 11.4 Write CLI integration tests
    - Test `tkr init --platform ecs` generates valid TOML
    - Test CLI parse for ECS-specific commands
    - Test invalid service name produces helpful error
    - _Requirements: 8.1, 8.2, 8.3_

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
