# Requirements Document: Orchestrator Framework

## Introduction

Tokeira currently has no deployment or infrastructure orchestration capability. The `temporal-dsql-deploy-eks` project contains a proven set of generic orchestration crates — a CAS state store, an infrastructure-as-code engine, a service deployment engine, and a TOML config loader — alongside AWS and Kubernetes provider implementations. These generic crates are decoupled from the concrete EKS deployment but live in a separate repository.

This spec migrates the reusable orchestration framework into the tokeira workspace as first-class workspace members under `crates/`, adds a Docker Compose provider for local development, adds a filesystem-backed state backend, creates the `Deployment` trait abstraction, and delivers a working local development deployment with a CLI. The goal is a self-contained tokeira repo where generic framework crates live in `crates/`, provider crates live in `crates/`, and concrete deployments live in `deployments/`.

The migration is organized into 5 phases:

- Phase 1: Migrate generic crates (state, iac, deploy-engine, config-loader) — no AWS dependency
- Phase 2: Create orchestrator crate with Deployment/Ops traits and generic engine facades
- Phase 3: Migrate AWS provider (subset of resources tokeira needs)
- Phase 4: Create Docker Compose provider for local development
- Phase 5: Create local deployment + CLI

### What This Spec Covers

1. Migration of the generic CAS state store with pluggable backends (S3 + local filesystem)
2. Migration of the generic infrastructure-as-code engine (Resource, Module, Engine, diff)
3. Migration of the generic service deployment engine (Service, Image, Platform, DeployEngine)
4. Migration of the generic TOML config loader (profile merge, variable substitution)
5. Creation of the orchestrator crate (Deployment trait, Ops trait, engine facades)
6. Migration of the AWS provider (VPC, Security Groups, S3, DynamoDB, IAM, DSQL, ECR only)
7. Creation of the Docker Compose provider (Resource implementations + Platform)
8. Creation of the local development deployment (compose + local state)
9. Creation of the deployment CLI (`tkr`)

### What This Spec Does NOT Cover

- The ECS deployment (`deployments/ecs/`) — future work
- The EKS deployment (`deployments/eks/`) — future work
- The Kubernetes provider (`tokeira-k8s`) — not needed for local dev or ECS
- Image build pipeline (`build`/`dagger-client` crates) — not needed for MVP
- Production deployment tooling — this spec focuses on the framework + local dev only
- The tokeirad configuration foundation (separate spec: configuration-foundation)
- Observability instrumentation (separate spec: observability-foundation)

## Glossary

- **State_Store**: A generic compare-and-swap (CAS) persistence layer parameterized by document type `T`. Persists any `Serialize + DeserializeOwned + Default + Validate` document using CAS-updated manifests and immutable snapshots.
- **State_Backend**: A trait abstracting the storage medium for State_Store. Implementations include S3_Backend (AWS S3) and Local_Backend (local filesystem).
- **S3_Backend**: A State_Backend implementation that persists state documents to Amazon S3.
- **Local_Backend**: A State_Backend implementation that persists state documents to the local filesystem.
- **IAC_Engine**: The generic infrastructure-as-code engine that plans, applies, and destroys infrastructure resources with dependency ordering.
- **Resource**: A trait representing a single infrastructure resource with create, update, delete, describe, and diff operations.
- **Module**: A trait representing a named deployment unit composed of Resources, with declared dependencies on other Modules.
- **Provision_Context**: A typed extension map passed through the IAC_Engine during provisioning, allowing Modules to share outputs (e.g., a VPC ID produced by one Module consumed by another).
- **Deploy_Engine**: The generic service deployment engine that plans and applies service deployments using the Platform trait.
- **Service**: A trait representing a deployable service that produces deployment manifests.
- **Image**: A trait representing a named deployable artifact with a source type (registry, local build).
- **Platform**: A trait representing a deployment target that applies service manifests (e.g., Kubernetes, Docker Compose, ECS).
- **Service_Context**: A typed extension map passed through the Deploy_Engine during service deployment.
- **Image_Context**: A typed extension map passed through the Deploy_Engine during image operations.
- **Deployment**: A trait that concrete deployment targets implement, binding together infrastructure modules, services, state stores, and configuration. Parameterizes the orchestrator's engine facades.
- **Ops**: A trait that concrete deployment targets implement for operational commands: scaling, log streaming, port forwarding.
- **Infra_Engine**: A generic engine facade parameterized by Deployment that orchestrates infrastructure plan/apply/destroy.
- **Config_Loader**: The generic TOML configuration loading module that loads a base config file, optionally deep-merges a profile overlay, substitutes project variables, and validates the result.
- **Deep_Merge**: The operation of recursively merging two TOML value trees where tables merge key-by-key and leaf values in the overlay replace the base.
- **Compose_Provider**: A provider crate implementing Resource and Platform traits for Docker Compose, enabling local development without cloud infrastructure.
- **Local_Deployment**: A concrete Deployment implementation using the Compose_Provider and Local_Backend for local development.
- **TKR_CLI**: The deployment command-line interface (`apps/tkr/`) parameterized by the Deployment trait, providing infra and deploy commands.

## Requirements

---

## Phase 1: Migrate Generic Crates

### Requirement 1.1: Generic CAS State Store

**User Story:** As a deployment developer, I want a generic compare-and-swap state store with pluggable backends, so that deployment state can be persisted to S3 in production or the local filesystem during development.

#### Acceptance Criteria

1. THE State_Store SHALL persist any document type `T` that implements `Serialize + DeserializeOwned + Default + Validate`, using CAS-updated manifests and immutable snapshots.
2. THE State_Store SHALL be parameterized by a State_Backend trait, allowing the storage medium to be selected at construction time.
3. THE State_Backend trait SHALL define async operations for reading, writing, and listing state objects, with each operation returning a `Result` with the unified `StateError` enum, ensuring the trait is object-safe and can be used as `Box<dyn StateBackend>`.
4. WHEN a CAS update detects a version conflict, THE State_Store SHALL return a `StateError::Conflict` error containing the current version, enabling the caller to retry with the latest state.
5. THE S3_Backend SHALL implement State_Backend within the `tokeira-state` crate, gated behind a `feature = "s3"` cargo feature that brings in the `aws-sdk-s3` dependency. When the feature is disabled, the S3_Backend type is not compiled.
6. THE Local_Backend SHALL implement State_Backend by persisting state objects to a configurable directory on the local filesystem, using atomic file writes to prevent partial state corruption.
7. THE Local_Backend SHALL use file-level locking or atomic rename to ensure that concurrent writes do not produce corrupted state files.
8. THE State_Store SHALL live in `crates/tokeira-state/` as a workspace member with the crate name `tokeira-state`.
9. FOR ALL valid state documents, writing a document to the State_Store and then reading the same key SHALL return a document equal to the original (round-trip property).

### Requirement 1.2: Generic Infrastructure-as-Code Engine

**User Story:** As a deployment developer, I want a generic infrastructure engine that plans, applies, and destroys resources with dependency ordering, so that infrastructure provisioning is consistent across providers.

#### Acceptance Criteria

1. THE Resource trait SHALL define async methods for `create`, `update`, `delete`, `describe`, and `diff`, each receiving a Provision_Context and returning a `Result`.
2. THE Module trait SHALL define a `name` method returning a unique string identifier, a `resources` method returning the Module's Resource list, and a `dependencies` method returning the names of Modules that must be provisioned first.
3. THE IAC_Engine SHALL resolve Module dependencies into a topological order and provision Modules in that order during `apply`.
4. WHEN a dependency cycle is detected among Modules, THE IAC_Engine SHALL return an error identifying the cycle rather than entering an infinite loop.
5. THE IAC_Engine SHALL support three operations: `plan` (compute diff without side effects), `apply` (provision resources), and `destroy` (tear down resources in reverse dependency order).
6. THE Provision_Context SHALL be a typed extension map that allows Modules to insert and retrieve typed values, enabling output sharing between Modules (e.g., a VPC ID produced by one Module consumed by another).
7. THE IAC_Engine SHALL include a diff engine that compares current state against desired state and produces a human-readable plan showing resources to create, update, or delete.
8. THE IAC_Engine SHALL live in `crates/tokeira-iac/` as a workspace member with the crate name `tokeira-iac`.

### Requirement 1.3: Generic Service Deployment Engine

**User Story:** As a deployment developer, I want a generic service deployment engine that plans and applies service deployments across different platforms, so that the same deployment logic works for Kubernetes, Docker Compose, and ECS.

#### Acceptance Criteria

1. THE Service trait SHALL define a method that produces deployment manifests from a Service_Context.
2. THE Image trait SHALL define a `name` method returning the image identifier and a `source` method describing the image source (registry reference, local build path).
3. THE Platform trait SHALL define an async `apply` method that takes deployment manifests and applies them to the target platform.
4. THE Deploy_Engine SHALL orchestrate service deployment by resolving images, generating manifests via Service, and applying them via Platform.
5. THE Service_Context SHALL be a typed extension map that allows services to share deployment-time values.
6. THE Image_Context SHALL be a typed extension map that allows image operations to share build-time values.
7. THE Deploy_Engine SHALL live in `crates/tokeira-deploy-engine/` as a workspace member with the crate name `tokeira-deploy-engine`.
8. THE crate name `tokeira-deploy-engine` SHALL NOT conflict with the existing `tokeira-runtime` crate, which is the workflow runtime.

### Requirement 1.4: Generic TOML Config Loader

**User Story:** As a deployment developer, I want a generic TOML config loader with profile deep-merge and variable substitution, so that deployment configurations can be composed from a base config and environment-specific overlays.

#### Acceptance Criteria

1. THE Config_Loader SHALL provide a `load_config(base_path, profile_path)` function that reads a base TOML file and optionally deep-merges a profile TOML overlay on top.
2. THE Deep_Merge operation SHALL merge TOML tables recursively key-by-key, with leaf values in the profile overlay replacing the corresponding base values.
3. THE Config_Loader SHALL support variable substitution in string values, replacing `{project}` and other declared placeholders with their resolved values after merge.
4. THE Config_Loader SHALL validate the merged configuration using a caller-provided validation function, returning all validation errors rather than stopping at the first.
5. THE Config_Loader SHALL be generic over the configuration type `T: DeserializeOwned`, so that each deployment defines its own config model.
6. THE Config_Loader SHALL provide a `write_config_values` function that serializes a configuration back to TOML for inspection.
7. THE Config_Loader SHALL live in `crates/tokeira-config-loader/` as a workspace member with the crate name `tokeira-config-loader`.
8. FOR ALL valid TOML configuration values, serializing to TOML with `write_config_values` and then loading with `load_config` SHALL produce an equivalent configuration (round-trip property).

---

## Phase 2: Orchestrator Crate

### Requirement 2.1: Deployment Trait

**User Story:** As a deployment developer, I want a Deployment trait that concrete deployment targets implement, so that the orchestrator's engine facades are generic over the deployment target.

#### Acceptance Criteria

1. THE Deployment trait SHALL define an associated type `Config` representing the deployment's configuration model, which must be `Send + Sync + Clone + 'static`.
2. THE Deployment trait SHALL define a method to list infrastructure modules in dependency order, returning `Vec<Box<dyn Module>>`.
3. THE Deployment trait SHALL define a method to list services to deploy, returning `Vec<Box<dyn Service>>`.
4. THE Deployment trait SHALL define a method to create an infrastructure state store, returning a `Box<dyn StateBackend>` rather than assuming S3.
5. THE Deployment trait SHALL define a method to create a deployment state store, returning a `Box<dyn StateBackend>` rather than assuming S3.
6. THE Deployment trait SHALL define a method to create a Provision_Context populated with deployment-specific extensions.
7. THE Deployment trait SHALL define a method to return a remote-state Module that provisions the storage backend (e.g., an S3 bucket for cloud deployments, a local directory for dev deployments), following the remote-state module → resource → state store lifecycle from the deploy-eks architecture.
8. THE Deployment trait SHALL live in `crates/tokeira-orchestrator/` as a workspace member with the crate name `tokeira-orchestrator`.

### Requirement 2.2: Ops Trait

**User Story:** As a deployment developer, I want an Ops trait for operational commands, so that each deployment target can implement scaling, log streaming, and port forwarding appropriate to its platform.

#### Acceptance Criteria

1. THE Ops trait SHALL define async methods for: `scale_up`, `scale_down`, `logs` (stream logs for a named service), and `port_forward` (forward a local port to a named service).
2. THE Ops trait SHALL receive the deployment's Config as a parameter, so that operational commands use the same configuration as provisioning.
3. WHEN a deployment target does not support an operational command, THE Ops implementation SHALL return a descriptive "not supported" error rather than silently succeeding.
4. THE Ops trait SHALL live in `crates/tokeira-orchestrator/` alongside the Deployment trait.

### Requirement 2.3: Generic Engine Facades

**User Story:** As a deployment developer, I want generic InfraEngine and DeployEngine facades parameterized by Deployment, so that the CLI and other consumers interact with a single entry point regardless of the deployment target.

#### Acceptance Criteria

1. THE Infra_Engine facade SHALL be parameterized by a type implementing Deployment, and SHALL delegate to the IAC_Engine using the Deployment's modules, state store, and provision context.
2. THE Infra_Engine facade SHALL expose `plan`, `apply`, and `destroy` methods that load state, invoke the IAC_Engine, and persist updated state.
3. THE Infra_Engine facade's `compose` method SHALL always prepend the remote-state module from `Deployment::remote_state_module()` ahead of the selected infrastructure modules, ensuring the state backend is provisioned before any other module runs.
3. THE Deploy_Engine facade SHALL be parameterized by a type implementing Deployment, and SHALL delegate to the generic Deploy_Engine using the Deployment's services and platform.
4. THE Deploy_Engine facade SHALL expose `plan` and `apply` methods that load state, invoke the service Deploy_Engine, and persist updated state.
5. THE engine facades SHALL live in `crates/tokeira-orchestrator/` alongside the Deployment and Ops traits.

---

## Phase 3: AWS Provider

### Requirement 3.1: AWS Resource Implementations

**User Story:** As a deployment developer, I want AWS resource implementations for the infrastructure tokeira needs, so that production deployments can provision cloud resources through the IAC engine.

#### Acceptance Criteria

1. THE AWS provider SHALL implement the Resource trait for: VPC, Security Groups, S3 Buckets, DynamoDB Tables, IAM Roles, DSQL Clusters, and ECR Repositories.
2. THE AWS provider SHALL NOT include Resource implementations for EKS, OpenSearch, Pod Identity, or VPC Endpoints — those are not needed by tokeira.
3. EACH AWS Resource implementation SHALL support `create`, `update`, `delete`, `describe`, and `diff` operations using the AWS SDK for Rust.
4. EACH AWS Resource implementation SHALL use the Provision_Context to read inputs from upstream resources (e.g., VPC ID for Security Group creation) and to publish its own outputs for downstream consumers.
5. THE AWS provider SHALL live in `crates/tokeira-aws/` as a workspace member with the crate name `tokeira-aws`.
6. THE AWS provider SHALL depend on `tokeira-iac` for the Resource and Provision_Context types, and SHALL NOT depend on any concrete deployment crate.

### Requirement 3.2: S3 State Backend Integration

**User Story:** As a deployment developer, I want the S3 state backend to use the AWS provider's credential and client configuration, so that state persistence shares the same AWS session as infrastructure provisioning.

#### Acceptance Criteria

1. THE S3_Backend SHALL accept an AWS SDK client configuration at construction time rather than creating its own, enabling shared credential management with the AWS provider.
2. THE S3_Backend SHALL support configurable bucket name and key prefix for state object storage.
3. WHEN S3 operations fail due to access denied or missing bucket, THE S3_Backend SHALL return descriptive errors including the bucket name and key path.

---

## Phase 4: Docker Compose Provider

### Requirement 4.1: Compose Resource Implementations

**User Story:** As a deployment developer, I want Docker Compose resource implementations, so that local development infrastructure can be provisioned through the same IAC engine used for cloud deployments.

#### Acceptance Criteria

1. THE Compose_Provider SHALL implement the Resource trait for compose services, using the `bollard` crate (Docker Engine API client) for all container lifecycle operations rather than shelling out to the `docker compose` CLI.
2. THE Compose_Provider SHALL support defining services with image, ports, volumes, environment variables, depends_on, and healthcheck configurations.
3. THE Compose_Provider `describe` operation SHALL inspect running containers via `bollard::Docker::list_containers` with label-based filtering (`com.docker.compose.service`) to determine current state.
4. THE Compose_Provider `diff` operation SHALL compare the desired service configuration against the running container state retrieved via bollard.
5. THE Compose_Provider `create` and `update` operations SHALL use bollard to create/start containers matching the desired configuration, and update the `docker-compose.yml` file as the desired-state artifact.
6. THE Compose_Provider `delete` operation SHALL use bollard to stop and remove the target container, then remove the service entry from the compose file and reconcile the running stack.
7. THE Compose_Provider SHALL live in `crates/tokeira-compose/` as a workspace member with the crate name `tokeira-compose`.

### Requirement 4.2: Compose Platform Implementation

**User Story:** As a deployment developer, I want a Platform implementation for Docker Compose, so that service deployments can target a local compose stack through the same Deploy_Engine used for cloud platforms.

#### Acceptance Criteria

1. THE Compose_Provider SHALL implement the Platform trait, applying service deployment manifests by reconciling desired state against running containers via the bollard Docker Engine API.
2. THE Compose Platform SHALL support scaling services by creating or removing container instances via bollard to match the desired count.
3. THE Compose Platform SHALL support log streaming by using `bollard::Docker::logs` with follow mode for the target container.
4. THE Compose Platform SHALL support port forwarding by reading the port mappings from the running container's configuration via bollard and reporting them to the caller.
5. IF the Docker Engine is not reachable via the bollard socket connection, THEN THE Compose_Provider SHALL return a descriptive error indicating that Docker is required.

---

## Phase 5: Local Deployment and CLI

### Requirement 5.1: Local Development Deployment

**User Story:** As a tokeira developer, I want a local deployment that stands up tokeirad with an observability stack using Docker Compose, so that I can develop and test locally without cloud infrastructure.

#### Acceptance Criteria

1. THE Local_Deployment SHALL implement the Deployment trait using the Compose_Provider for infrastructure and the Local_Backend for state persistence.
2. THE Local_Deployment SHALL define infrastructure modules for: tokeirad service, Mimir (metrics storage), Grafana, Loki, and Alloy (collection agent).
3. THE Local_Deployment SHALL configure Alloy to scrape the tokeirad metrics endpoint and remote-write to Mimir.
4. THE Local_Deployment SHALL configure Grafana with provisioned datasources for Mimir and Loki.
5. THE Local_Deployment SHALL configure Alloy to collect container logs and ship them to Loki.
6. THE Local_Deployment SHALL store its state in a `.tokeira-state/` directory within the workspace, using the Local_Backend.
7. THE Local_Deployment SHALL live in `deployments/local/` as a workspace member.
8. THE Local_Deployment SHALL define its own `Config` type (the Deployment associated type) loaded via the Config_Loader from a TOML file in `deployments/local/config/`.

### Requirement 5.2: Deployment CLI

**User Story:** As a tokeira developer, I want a deployment CLI that provides infrastructure and service management commands, so that I can provision, deploy, and operate tokeira environments from the command line.

#### Acceptance Criteria

1. THE TKR_CLI SHALL provide infrastructure commands: `infra plan`, `infra apply`, and `infra destroy`.
2. THE TKR_CLI SHALL provide deployment commands: `deploy plan` and `deploy apply`.
3. THE TKR_CLI SHALL provide operational commands: `scale up`, `scale down`, `logs <service>`, and `port-forward <service> <port>`.
4. THE TKR_CLI SHALL provide configuration commands: `config init` (generate a default config file) and `config dump` (print resolved config).
5. THE TKR_CLI SHALL accept a `--deployment` argument selecting the deployment target (default: `local`).
6. THE TKR_CLI SHALL accept a `--config` argument specifying the config file path, and a `--profile` argument for profile overlay.
7. THE TKR_CLI SHALL display a human-readable plan for `infra plan` and `deploy plan`, and require explicit confirmation (or `--yes` flag) before `infra apply`, `infra destroy`, and `deploy apply`.
8. THE TKR_CLI SHALL live in `apps/tkr/` as a workspace member.
9. THE TKR_CLI SHALL be parameterized by the Deployment trait, so that adding a new deployment target requires no changes to the CLI crate itself.

### Requirement 5.3: Local Deployment Ops

**User Story:** As a tokeira developer, I want operational commands for the local deployment, so that I can scale services, stream logs, and forward ports during local development.

#### Acceptance Criteria

1. THE Local_Deployment SHALL implement the Ops trait with `scale_up` and `scale_down` using bollard to create or remove container instances for the target service.
2. THE Local_Deployment SHALL implement the Ops trait with `logs` using `bollard::Docker::logs` with follow mode for the target container.
3. THE Local_Deployment SHALL implement the Ops trait with `port_forward` reading port mappings from the running container's configuration via bollard and reporting them to the caller.
4. WHEN a service name is not recognized, THE Local_Deployment Ops implementation SHALL return an error listing the valid service names.

---

## Cross-Cutting Requirements

### Requirement 6.1: Crate Naming and Workspace Integration

**User Story:** As a tokeira contributor, I want all migrated crates to follow tokeira naming conventions and integrate cleanly into the workspace, so that the codebase is consistent and discoverable.

#### Acceptance Criteria

1. ALL migrated crates SHALL use the `tokeira-` prefix in their crate names: `tokeira-state`, `tokeira-iac`, `tokeira-deploy-engine`, `tokeira-config-loader`, `tokeira-orchestrator`, `tokeira-aws`, `tokeira-compose`.
2. ALL new crates SHALL be added as workspace members in the root `Cargo.toml`.
3. THE generic crates (`tokeira-state`, `tokeira-iac`, `tokeira-deploy-engine`, `tokeira-config-loader`, `tokeira-orchestrator`) SHALL NOT depend on any provider crate (`tokeira-aws`, `tokeira-compose`) or deployment crate (`deployments/*`).
4. THE provider crates (`tokeira-aws`, `tokeira-compose`) SHALL depend on `tokeira-iac` and optionally `tokeira-deploy-engine`, but SHALL NOT depend on `tokeira-orchestrator` or any deployment crate.
5. THE deployment crates (`deployments/local/`) SHALL depend on `tokeira-orchestrator` and the provider crates they use, but SHALL NOT be depended upon by any crate in `crates/`.

### Requirement 6.2: Dependency Layering

**User Story:** As a tokeira contributor, I want a clear dependency hierarchy between framework, provider, and deployment crates, so that the architecture remains modular and provider-agnostic.

#### Acceptance Criteria

1. THE dependency hierarchy SHALL follow: generic crates → provider crates → deployment crates → CLI, with no reverse dependencies.
2. THE `tokeira-orchestrator` crate SHALL depend on `tokeira-state`, `tokeira-iac`, and `tokeira-deploy-engine` for trait definitions, but SHALL NOT depend on any provider or deployment crate.
3. THE `tokeira-aws` crate SHALL depend on `tokeira-iac` and `tokeira-state` (for S3_Backend), but SHALL NOT depend on `tokeira-orchestrator`.
4. THE `tokeira-compose` crate SHALL depend on `tokeira-iac` and `tokeira-deploy-engine` (for Platform), but SHALL NOT depend on `tokeira-orchestrator`.
5. THE `apps/tkr/` CLI SHALL depend on `tokeira-orchestrator` and on the deployment crates it supports, wiring them together at the binary level.

### Requirement 6.3: Error Handling

**User Story:** As a deployment developer, I want consistent error handling across all orchestrator crates, so that errors are descriptive, composable, and easy to diagnose.

#### Acceptance Criteria

1. EACH orchestrator crate SHALL define its own error enum using `thiserror`, with variants for each distinct failure mode.
2. WHEN an error originates from a downstream crate (e.g., AWS SDK, filesystem I/O, TOML parsing), THE error variant SHALL wrap the source error using `#[from]` or `#[source]` for error chain preservation.
3. THE IAC_Engine SHALL propagate Resource errors with the resource name and module name attached, so that operators can identify which resource in which module failed.
4. THE State_Store SHALL propagate backend errors with the key path attached, so that operators can identify which state object caused the failure.
