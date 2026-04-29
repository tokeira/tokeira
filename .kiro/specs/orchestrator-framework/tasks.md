# Implementation Plan: Orchestrator Framework

## Overview

Migrate the proven orchestration framework from `temporal-dsql-deploy-eks` into the tokeira workspace, add a Docker Compose provider and local filesystem state backend, create the `Deployment` trait abstraction, and deliver a working local development deployment with a CLI. The implementation follows 5 phases with checkpoints after each.

Source crates live at `../../temporalio/temporal-dsql-deploy-eks/crates/`. All `dsqld_` prefixed crate names must be renamed to `tokeira_` during migration. Only the generic parts of each crate migrate — deployment-specific logic stays behind.

## Tasks

- [x] 1. Phase 1 — Migrate generic crates
  - [x] 1.1 Create `crates/tokeira-state/` with `StateBackend` trait, `StateStore<T>`, and `LocalBackend`
    - Migrate from `../../temporalio/temporal-dsql-deploy-eks/crates/state/` (~684 lines)
    - Rename all `dsqld_` references to `tokeira_`
    - Define `StateBackend` trait with `read_manifest`, `write_manifest`, `read_snapshot`, `write_snapshot`, `list_snapshots`
    - Define `Validate` trait for post-deserialization integrity checks
    - Implement `StateStore<T>` with `load()` and `save()` using CAS semantics (manifest + snapshot model, SHA-256 checksums)
    - Implement `LocalBackend` using `tempfile` + atomic rename for writes, advisory file locking (`flock`) for CAS on manifests
    - Define `StateError` enum with `Conflict`, `NotFound`, `Validation`, `Backend` variants using `thiserror` — this is the unified error type for all backends, ensuring the `StateBackend` trait is object-safe
    - Implement `S3Backend` behind `feature = "s3"` cargo feature gating the `aws-sdk-s3` dependency — uses ETags for CAS on manifest writes, accepts pre-configured `aws_sdk_s3::Client`
    - Add `Cargo.toml` with workspace dependencies: `serde`, `serde_json`, `async-trait`, `thiserror`, `tokio`, `sha2`, `tempfile`; optional `aws-sdk-s3` behind `s3` feature
    - Add crate as workspace member in root `Cargo.toml`
    - _Requirements: 1.1.1, 1.1.2, 1.1.3, 1.1.4, 1.1.5, 1.1.6, 1.1.7, 1.1.8_

  - [ ]* 1.2 Write property tests for `tokeira-state`
    - **Property 1: State store round-trip** — generate random `InfraState`/`RuntimeState`, save/load via `StateStore<T>` with `LocalBackend` in tempdir, assert equality
    - **Validates: Requirements 1.1.1, 1.1.9**
    - **Property 2: CAS conflict detection** — save doc, modify store externally, attempt save with stale version, verify `ConflictError` with actual current version
    - **Validates: Requirements 1.1.4**
    - **Property 6: Local backend atomic writes** — spawn concurrent save tasks to the same local path, assert exactly one succeeds and the file is valid
    - **Validates: Requirements 1.1.6, 1.1.7**

  - [x] 1.3 Create `crates/tokeira-iac/` with `Resource`, `Module`, `Engine`, `ProvisionContext`, and diff types
    - Migrate from `../../temporalio/temporal-dsql-deploy-eks/crates/iac/` (~1871 lines)
    - Rename all `dsqld_` references to `tokeira_`
    - Define `Resource` trait with async `create`, `update`, `delete`, `describe`, `diff` methods
    - Define `Module` trait with `name`, `dependencies`, `resources` methods
    - Implement `ProvisionContext` as a typed extension map (`TypeId → Box<dyn Any + Send + Sync>`) with `insert<T>`, `get<T>`, `get_mut<T>`, plus `tags`, `state`, `progress` fields
    - Implement `Engine` with `plan`, `apply`, `destroy` — topological sort for apply, reverse for destroy, cycle detection returning `DependencyCycle` error
    - Define diff types: `ChangeKind` (Create/Update/Delete/NoChange), `Change`, `FieldDiff`, `ResourceDiff`
    - Define `InfraComposition` and `ModuleSelection` (All/Only/Except)
    - Define `InfraState` (HashMap of resource name → JSON value) implementing `Validate`
    - Define `IacError` enum with `DependencyCycle`, `ResourceFailed { module, resource, source }`, `ModuleNotFound`, `State` variants
    - Add `Cargo.toml` depending on `tokeira-state` for state types
    - Add crate as workspace member in root `Cargo.toml`
    - _Requirements: 1.2.1, 1.2.2, 1.2.3, 1.2.4, 1.2.5, 1.2.6, 1.2.7, 1.2.8_

  - [ ]* 1.4 Write property tests for `tokeira-iac`
    - **Property 3: Topological ordering** — generate random DAGs via adjacency lists, verify apply order respects dependencies and destroy reverses it
    - **Validates: Requirements 1.2.3, 1.2.5**
    - **Property 4: Cycle detection** — generate random graphs with forced cycles, verify `DependencyCycle` error
    - **Validates: Requirements 1.2.4**
    - **Property 5: Typed extension map round-trip** — generate random `i64`, `String`, `Vec<u8>` values, insert/retrieve from `ProvisionContext`, assert equality
    - **Validates: Requirements 1.2.6**
    - **Property 6: Diff engine classification** — generate random current/desired `HashMap<String, Value>` pairs, verify Create/Delete/Update/NoChange classification
    - **Validates: Requirements 1.2.7**

  - [x] 1.5 Create `crates/tokeira-deploy-engine/` with `Service`, `Image`, `Platform`, `ServiceEngine`
    - Migrate from `../../temporalio/temporal-dsql-deploy-eks/crates/runtime/` (~469 lines) — this is the DEPLOYMENT runtime, not tokeira's workflow runtime
    - Rename all `dsqld_` references to `tokeira_`
    - Define `Service` trait with `name`, `module`, `dependencies`, async `manifests` methods
    - Define `Image` trait with `name`, `source_type`, `desired_ref` methods; define `ImageSourceType` enum (Registry, Build)
    - Define `Platform` trait with async `apply_manifests` method
    - Implement `ServiceContext` and `ImageContext` as typed extension maps (same pattern as `ProvisionContext`)
    - Define `RuntimeState` (services + images maps) implementing `Validate`
    - Implement `ServiceEngine` with `plan_services`, `apply_services`, `record_images`
    - Define `DeployError` enum with `ServiceFailed`, `PlatformFailed`, `State` variants
    - Add `Cargo.toml` — no dependency on `tokeira-state` (state types are local)
    - Add crate as workspace member in root `Cargo.toml`
    - _Requirements: 1.3.1, 1.3.2, 1.3.3, 1.3.4, 1.3.5, 1.3.6, 1.3.7, 1.3.8_

  - [x] 1.6 Create `crates/tokeira-config-loader/` with `load_config`, `deep_merge`, `substitute_vars`, `write_config_values`
    - Migrate generic loading machinery from `../../temporalio/temporal-dsql-deploy-eks/crates/config/` (~2827 lines, generic parts only)
    - Do NOT migrate `ProjectConfig` model — that is deployment-specific
    - Rename all `dsqld_` references to `tokeira_`
    - Implement `load_config<T: DeserializeOwned>(base_path, profile_path)` — reads base TOML, optionally deep-merges profile overlay, deserializes into `T`
    - Implement `deep_merge(base, overlay)` — recursive key-by-key merge, leaves override
    - Implement `substitute_vars(value, vars)` — replace `{project}` and other declared placeholders in string values
    - Implement `validate_config<T>(config, validator)` — call validator, return ALL errors not just the first
    - Implement `write_config_values<T: Serialize>(config)` — serialize back to TOML string
    - Define `ConfigLoaderError` enum with `ReadFile`, `Parse`, `Serialize`, `Validation` variants
    - Add `Cargo.toml` with `toml`, `serde`, `thiserror` dependencies
    - Add crate as workspace member in root `Cargo.toml`
    - _Requirements: 1.4.1, 1.4.2, 1.4.3, 1.4.4, 1.4.5, 1.4.6, 1.4.7_

  - [ ]* 1.7 Write property tests for `tokeira-config-loader`
    - **Property 7: Deep merge semantics** — generate random TOML value trees, merge, verify overlay wins for leaves, base preserved for absent keys, nested tables merge recursively
    - **Validates: Requirements 1.4.2**
    - **Property 8: Variable substitution completeness** — generate random TOML with `{project}` in strings, substitute, verify no placeholders remain
    - **Validates: Requirements 1.4.3**
    - **Property 9: Validation reports all errors** — generate configs with N failures, verify N errors returned
    - **Validates: Requirements 1.4.4**
    - **Property 10: Config TOML round-trip** — generate random config structs, serialize/deserialize, verify equality
    - **Validates: Requirements 1.4.8**

- [x] 2. Checkpoint — Phase 1 compilation and tests
  - Verify all 4 generic crates compile: `cargo build -p tokeira-state -p tokeira-iac -p tokeira-deploy-engine -p tokeira-config-loader`
  - Run `cargo lint` and `cargo +nightly fmt` across the workspace
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Phase 2 — Create orchestrator crate
  - [x] 3.1 Create `crates/tokeira-orchestrator/` with `Deployment` trait, `Ops` trait, and engine facades
    - Define `Deployment` trait with associated type `Config` and methods: `remote_state_module`, `infra_modules`, `services`, `images`, `required_namespaces`, `register_infra_extensions`, `register_deploy_extensions`, `create_infra_store`, `create_deploy_store`, `hydrate_config`, `collect_writeback`
    - The `remote_state_module` method returns a Module that provisions the storage backend (S3 bucket for cloud, local directory for dev), following the remote-state module → resource → state store lifecycle
    - The `create_infra_store` and `create_deploy_store` methods return `Box<dyn StateBackend>` — not S3-specific
    - Define `Ops` trait with `deployment_name`, `valid_services`, `service_namespace`, `port_forward_target`, `startup_replicas`, `job`; define `PortForwardTarget` and `ServiceReplicas` structs
    - Implement `InfraEngine<D: Deployment>` facade — wraps `iac::Engine`, loads/persists state via `StateStore<InfraState>`, exposes `compose`, `plan`, `apply`, `destroy`, `collect_writeback`
    - The `compose` method SHALL always prepend the remote-state module from `Deployment::remote_state_module()` ahead of the selected infrastructure modules, ensuring the state backend is provisioned before any other module
    - Implement `DeployEngine<D: Deployment>` facade — wraps `deploy_engine::ServiceEngine`, loads/persists state via `StateStore<RuntimeState>`, exposes `plan`, `apply`
    - Define `OrchestratorError` enum wrapping `IacError`, `DeployError`, `ConfigLoaderError`, `StateError`
    - Add `Cargo.toml` depending on `tokeira-state`, `tokeira-iac`, `tokeira-deploy-engine` — NO provider or deployment dependencies
    - Add crate as workspace member in root `Cargo.toml`
    - _Requirements: 2.1.1, 2.1.2, 2.1.3, 2.1.4, 2.1.5, 2.1.6, 2.1.7, 2.2.1, 2.2.2, 2.2.3, 2.2.4, 2.3.1, 2.3.2, 2.3.3, 2.3.4, 2.3.5_

  - [x]* 3.2 Write unit tests for `tokeira-orchestrator`
    - Test `InfraEngine` plan/apply/destroy with mock `Deployment` impl
    - Test `DeployEngine` plan/apply with mock `Deployment` impl
    - Test that unsupported `Ops` methods return descriptive errors
    - _Requirements: 2.1, 2.2, 2.3_

- [x] 4. Checkpoint — Phase 2 compilation and tests
  - Verify `cargo build -p tokeira-orchestrator` compiles
  - Verify dependency layering: orchestrator depends on state/iac/deploy-engine but NOT on any provider or deployment crate
  - Run `cargo lint` and `cargo +nightly fmt`
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Phase 3 — Migrate AWS provider
  - [ ] 5.1 Create `crates/tokeira-aws/` with AWS resource implementations
    - Migrate subset from `../../temporalio/temporal-dsql-deploy-eks/crates/aws/` (~7582 lines, subset only)
    - Rename all `dsqld_` references to `tokeira_`
    - Include ONLY: `VpcResource`, `SecurityGroupResource`, `S3BucketResource`, `DynamoDbTableResource`, `IamRoleResource`, `DsqlClusterResource`, `EcrRepositoryResource`
    - Do NOT include: EKS, OpenSearch, Pod Identity, VPC Endpoints
    - Each resource implements `iac::Resource` trait with `create`, `update`, `delete`, `describe`, `diff`
    - Each resource uses `ProvisionContext` to read inputs from upstream resources and publish outputs for downstream consumers
    - Define `AwsClients` struct holding SDK clients (ec2, s3, dynamodb, iam, dsql, ecr) — registered as a `ProvisionContext` extension
    - Define `AwsError` enum with `SdkError`, `ResourceNotFound` variants
    - Add `Cargo.toml` depending on `tokeira-iac`, `tokeira-state` — NOT on `tokeira-orchestrator` or any deployment crate
    - Add AWS SDK crates as dependencies (`aws-sdk-ec2`, `aws-sdk-s3`, `aws-sdk-dynamodb`, `aws-sdk-iam`, `aws-sdk-dsql`, `aws-sdk-ecr`, `aws-config`)
    - Add crate as workspace member in root `Cargo.toml`
    - _Requirements: 3.1.1, 3.1.2, 3.1.3, 3.1.4, 3.1.5, 3.1.6_

  - [ ]* 5.2 Write unit tests for `tokeira-aws`
    - Test each resource's `diff` logic with mock state
    - Test `AwsClients` registration on `ProvisionContext`
    - _Requirements: 3.1_

- [ ] 6. Checkpoint — Phase 3 compilation and tests
  - Verify `cargo build -p tokeira-aws` compiles
  - Verify dependency layering: `tokeira-aws` depends on `tokeira-iac` and `tokeira-state` but NOT on `tokeira-orchestrator`
  - Run `cargo lint` and `cargo +nightly fmt`
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Phase 4 — Create Docker Compose provider
  - [x] 7.1 Create `crates/tokeira-compose/` with compose resource implementations and platform
    - Implement `ComposeService` struct with `name`, `image`, `ports`, `volumes`, `environment`, `depends_on`, `healthcheck` fields
    - Implement `iac::Resource` for `ComposeService`: `create` uses bollard to create and start the container; `update` stops, removes, and recreates with new config; `delete` uses bollard to stop and remove the container, then removes the entry from the compose file; `describe` uses `bollard::Docker::list_containers` with label-based filtering to determine current state; `diff` compares desired config vs running container state from bollard
    - Implement `ComposePlatform` struct wrapping `bollard::Docker` with `compose_file` and `project_name` fields
    - Implement `deploy_engine::Platform` for `ComposePlatform`: `apply_manifests` merges manifests into docker-compose.yml (desired-state artifact) and reconciles via bollard container create/start
    - Label all managed containers with `com.docker.compose.service` and `com.docker.compose.project` for discovery
    - Add compose YAML serialization/deserialization (use `serde_yaml` or manual generation) for the desired-state artifact
    - Check Docker Engine reachability via bollard socket connection — return `ComposeError::DockerNotAvailable` if unreachable
    - Define `ComposeError` enum with `DockerNotAvailable`, `ContainerFailed`, `YamlError` variants
    - Add `Cargo.toml` depending on `tokeira-iac`, `tokeira-deploy-engine`, and `bollard` — NOT on `tokeira-orchestrator`
    - Add crate as workspace member in root `Cargo.toml`
    - _Requirements: 4.1.1, 4.1.2, 4.1.3, 4.1.4, 4.1.5, 4.1.6, 4.1.7, 4.2.1, 4.2.2, 4.2.3, 4.2.4, 4.2.5_

  - [x]* 7.2 Write property tests for `tokeira-compose`
    - **Property 11: Compose service serialization completeness** — generate random `ComposeService` structs, serialize to YAML, verify all fields present
    - **Validates: Requirements 4.1.2**
    - **Property 12: Port mapping extraction** — generate random compose configs with ports, extract port-forward targets, verify correctness
    - **Validates: Requirements 4.2.4**

  - [x]* 7.3 Write unit tests for `tokeira-compose`
    - Test compose YAML generation from `ComposeService` definitions
    - Test diff against existing compose file
    - Test create/update/delete service entries in compose file
    - Test Docker Engine not-reachable detection via bollard
    - _Requirements: 4.1, 4.2_

- [x] 8. Checkpoint — Phase 4 compilation and tests
  - Verify `cargo build -p tokeira-compose` compiles
  - Verify dependency layering: `tokeira-compose` depends on `tokeira-iac` and `tokeira-deploy-engine` but NOT on `tokeira-orchestrator`
  - Run `cargo lint` and `cargo +nightly fmt`
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Phase 5 — Create local deployment and CLI
  - [x] 9.1 Create `deployments/local/` with `LocalDeployment` implementing `Deployment` and `Ops`
    - Create `deployments/local/Cargo.toml` depending on `tokeira-orchestrator`, `tokeira-compose`, `tokeira-config-loader`, `tokeira-state`
    - Implement `LocalDeployment` struct implementing `Deployment` trait:
      - `type Config = LocalConfig`
      - `remote_state_module` returns a `LocalStateModule` that ensures the state directory exists
      - `infra_modules` returns compose modules for: tokeirad, mimir (grafana/mimir:3.0.6), grafana (grafana/grafana-oss:12.4.3), loki (grafana/loki:3.7.1), alloy (grafana/alloy:v1.16.0)
      - `create_infra_store` / `create_deploy_store` return `Box::new(LocalBackend::new(&config.state_dir))`
      - Configure Alloy to scrape tokeirad metrics endpoint and remote-write to Mimir
      - Configure Grafana with provisioned datasources for Mimir and Loki
      - Configure Alloy to collect container logs and ship to Loki
    - Implement `Ops` for `LocalDeployment`:
      - `valid_services` returns `["tokeirad", "mimir", "grafana", "loki", "alloy"]`
      - `scale_up`/`scale_down` use bollard to create or remove container instances for the target service
      - `logs` uses `bollard::Docker::logs` with follow mode for the target container
      - `port_forward` reads port mappings from the running container's configuration via bollard
      - Unknown service name returns error listing valid service names
    - Define `LocalConfig` struct with `project_name`, `state_dir`, `compose_file`, `tokeirad` (TokeiradServiceConfig), `observability` (ObservabilityConfig)
    - Create default config TOML at `deployments/local/config/config.toml` with pinned image versions
    - State stored in `.tokeira-state/` directory
    - Add crate as workspace member in root `Cargo.toml`
    - _Requirements: 5.1.1, 5.1.2, 5.1.3, 5.1.4, 5.1.5, 5.1.6, 5.1.7, 5.1.8, 5.3.1, 5.3.2, 5.3.3, 5.3.4_

  - [x]* 9.2 Write property test for local deployment ops
    - **Property 13: Invalid service name error includes valid alternatives** — generate random invalid names, verify error lists all valid service names
    - **Validates: Requirements 5.3.4**

  - [x] 9.3 Create `apps/tkr/` deployment CLI
    - Create `apps/tkr/Cargo.toml` depending on `tokeira-orchestrator`, `tokeira-config-loader`, and `deployments/local/` (as `tokeira-local-deployment` or path dep)
    - Implement `Cli` struct with `clap::Parser`: `--deployment` (default: "local"), `--config`, `--profile`, subcommands
    - Implement `Command` enum: `Infra { Plan | Apply { --yes } | Destroy { --yes } }`, `Deploy { Plan | Apply { --yes } }`, `Scale { Up | Down }`, `Logs { service }`, `PortForward { service, port }`, `Config { Init | Dump }`
    - Implement deployment-agnostic `run<D: Deployment + Ops>` function that loads config via `load_config`, constructs deployment, and dispatches commands
    - Wire `main()` to match on `--deployment` and construct the appropriate `Deployment` impl (currently only "local")
    - Display human-readable plan for `infra plan` and `deploy plan`
    - Require explicit confirmation (or `--yes` flag) before `infra apply`, `infra destroy`, `deploy apply`
    - Add crate as workspace member in root `Cargo.toml`
    - _Requirements: 5.2.1, 5.2.2, 5.2.3, 5.2.4, 5.2.5, 5.2.6, 5.2.7, 5.2.8, 5.2.9_

  - [x]* 9.4 Write unit tests for CLI argument parsing and local deployment
    - Test each CLI subcommand parses correctly
    - Test `LocalDeployment` returns correct modules, services, state backend type
    - Test unknown deployment target returns descriptive error
    - _Requirements: 5.1, 5.2_

- [x] 10. Checkpoint — Phase 5 compilation and tests
  - Verify `cargo build -p tokeira-local-deployment -p tkr` compiles (adjust crate names as needed)
  - Verify dependency layering: `deployments/local/` depends on orchestrator + compose + config-loader; `apps/tkr/` depends on orchestrator + local deployment
  - Run `cargo lint` and `cargo +nightly fmt`
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 11. Cross-cutting verification
  - [x] 11.1 Verify crate naming and workspace integration
    - Confirm all 7 new crates use `tokeira-` prefix: `tokeira-state`, `tokeira-iac`, `tokeira-deploy-engine`, `tokeira-config-loader`, `tokeira-orchestrator`, `tokeira-aws`, `tokeira-compose`
    - Confirm all crates plus `deployments/local/` and `apps/tkr/` are listed as workspace members in root `Cargo.toml`
    - _Requirements: 6.1.1, 6.1.2_
  - [ ] 11.2 Verify dependency layering
    - Confirm generic crates do NOT depend on any provider or deployment crate (_Requirements: 6.1.3, 6.2.1, 6.2.2_)
    - Confirm provider crates depend on `tokeira-iac`/`tokeira-deploy-engine`/`tokeira-state` but NOT on `tokeira-orchestrator` or deployment crates (_Requirements: 6.1.4, 6.2.3, 6.2.4_)
    - Confirm deployment crates depend on `tokeira-orchestrator` + providers but are NOT depended upon by any crate in `crates/` (_Requirements: 6.1.5, 6.2.5_)
    - _Requirements: 6.1.3, 6.1.4, 6.1.5, 6.2.1, 6.2.2, 6.2.3, 6.2.4, 6.2.5_
  - [x]* 11.3 Write property tests for error context enrichment
    - **Property 14: Error context enrichment** — trigger resource failures in IAC engine, verify propagated error contains resource name and module name; trigger backend failures in State store, verify error contains key path
    - **Validates: Requirements 6.3.3, 6.3.4**

- [ ] 12. Final checkpoint — Full workspace build and test
  - Run `cargo build` for the entire workspace
  - Run `cargo lint` and `cargo +nightly fmt`
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation after each phase
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- The source code uses `dsqld_` prefixed crate names — all must be renamed to `tokeira_` during migration
- The `runtime` crate in deploy-eks is the DEPLOYMENT runtime (Service/Image/Platform), NOT tokeira's workflow runtime — it becomes `tokeira-deploy-engine`
- The `config` crate's `ProjectConfig` model is deployment-specific and stays out — only the generic loading machinery migrates
- The `aws` crate should only include: VPC, Security Groups, S3 Buckets, DynamoDB Tables, IAM Roles, DSQL Clusters, ECR — NOT EKS, OpenSearch, Pod Identity, VPC Endpoints
- The observability stack uses Mimir (not Prometheus), Alloy, Loki, Grafana — pinned to specific versions
- Use `cargo lint` for clippy, `cargo +nightly fmt` for formatting
- Comments should explain WHY, not WHAT
