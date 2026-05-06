# Requirements Document: Compose DSQL Persistence

## Introduction

This document captures the requirements for adding Aurora DSQL persistence support to the compose platform. Currently the compose platform supports only in-memory storage (`--storage in-memory`). This spec adds DSQL as a storage option for compose deployments, covering:

1. A new `dsql` IaC module that provisions a DSQL cluster (managed mode) or adopts a pre-existing cluster (preexisting mode via endpoint config).
2. Config wiring so `ComposeConfig` gains DSQL fields and the provisioned endpoint is written back to `tokeirad.toml` via the existing writeback machinery.
3. Wiring `tkr schema setup` into the compose platform lifecycle so it executes against the provisioned/configured endpoint (the `dsql-schema-connection` spec owns the schema tooling itself).
4. AWS credentials mounted into the `tokeirad` Docker Compose container so IAM auth works at runtime.
5. Lifecycle ordering: `tkr image build` → `tkr infra apply --module dsql` → `tkr schema setup` → `tkr infra apply` (remaining modules) → `tkr deploy apply`.

### Upstream dependencies

This spec assumes the following exist when implementation starts:

- **`tkr image build`** — produces `tokeirad:latest` in the local Docker image store. Required before `tkr deploy apply` because the compose platform's pre-deploy gate refuses to proceed if `tokeirad:latest` is absent. The `tokeirad` binary is a single executable with all storage backends compiled in, so the same image serves both in-memory and DSQL deployments. This command is delivered by the in-flight `image-lifecycle` work; this spec depends on it landing but does not define it.

All other dependencies are code that already exists in the workspace — see "What already exists" below.

### What already exists

- `tokeira-aws` crate with existing implementations of `AwsClients` (`crates/tokeira-aws/src/clients.rs`), `DsqlCluster` with the `DsqlClusterMode` enum (`crates/tokeira-aws/src/resources/dsql_cluster.rs`), and `DsqlConnectionEndpoint` (`crates/tokeira-aws/src/resources/dsql_connection_endpoint.rs`). The managed/preexisting mode convention is captured by the `effective_managed(config_mode, state_mode)` helper in `dsql_cluster.rs`.
- `tokeira-iac` crate with `Module` trait, plan/apply/destroy, `StateSaver`, and the `write_config_values(path, values)` helper plus `WritebackError` at `crates/tokeira-iac/src/writeback.rs`. Re-exported from `tokeira_iac::{WritebackError, write_config_values}`.
- `tokeira-orchestrator` with `StorageKind::{InMemory, Dsql}` and the `Deployment::collect_writeback(&self, config, &state) -> Vec<(String, String)>` trait method. The `tkr infra apply` handler already calls `engine.collect_writeback()` and pipes the result through `write_tokeirad_writeback` in `apps/tkr/src/commands/infra.rs`.
- `tokeira-config` with `DsqlInfraConfig` (`endpoint`, `region`, `admin_role_arn`, `runtime_role_arn`, `readonly_role_arn`) at `TokeiraConfig.infrastructure.dsql`, and a separate `DsqlCapacityConfig` (`max_connections`, `connection_rate_per_second`, `burst_capacity`) at `TokeiraConfig.capacity.dsql`. Both structs use `#[serde(deny_unknown_fields)]`.
- `tokeira-storage` crate with the DSQL storage foundation:
  - `DsqlStore` — **top-level facade** at `crates/tokeira-storage/src/dsql/mod.rs` with `DsqlStore::connect(auth, config)` and `DsqlStore::from_pool(pool, config)` constructors. Wires a shared `Arc<DsqlConnectionDirector>`, a `DsqlRunRepository`, a `DsqlProjectionLog`, and a `MigrationRunner` over one coordinated reservoir. Accessors: `connection_director()`, `migration_runner()`, `run_repository()`, `projection_log()`.
  - `MigrationRunner` — forward-only migration runner with `apply(pool)`, `dry_run(pool)`, `validate()`, `status(pool)`. Implemented in `crates/tokeira-storage/src/dsql/migration.rs`.
  - 46 migration files (V001–V046) covering the run-repository tables plus visibility (V012, V017, V029–V042) and routing/budget allocation (V043–V046), all under `crates/tokeira-storage/migrations/`.
  - `DsqlConnectionDirector` — reservoir pattern with class-based budgets, token-bucket rate limiter, proactive expiry scanner. Implemented in `crates/tokeira-storage/src/dsql/connection.rs` and `reservoir.rs`.
  - `DsqlAuthConfig` — IAM auth config with `endpoint`, `region`, and optional role ARNs. Falls back to the default credential chain when role ARNs are `None`. Includes `detect_region_from_endpoint` for `*.dsql.{region}.on.aws` parsing. Defined in `crates/tokeira-storage/src/dsql/config.rs`.
  - `DsqlPoolConfig` / `ReservoirConfig` / `MigrationConfig` — pool, reservoir, and migration-discovery tuning, all in `tokeira-storage::dsql::config`. The source comment states *"These settings are intentionally internal to the DSQL backend. The server configuration currently exposes only high-level deployment metadata"*.
  - `DsqlRunRepository` — `RunRepository` implementation over DSQL at `crates/tokeira-storage/src/dsql/run_repository.rs`. Comprehensively instrumented with 28 `#[instrument]`-annotated methods covering every public operation.
  - `DsqlProjectionLog` — `ProjectionLog` implementation over DSQL at `crates/tokeira-storage/src/dsql/projection_log.rs`. Reads committed projection operations for the worker's consumption.
- `tokeira-projection` crate with an **already-complete DSQL visibility store**:
  - `DsqlVisibilityStore` at `crates/tokeira-projection/src/dsql_store.rs` (1,983 lines, gated behind the `dsql` Cargo feature). Implements both `VisibilityStore` (`upsert_execution`, `delete_execution`, `list_executions`, `count_executions`, checkpoint read/write) and `ProjectionSink` (`apply`). Includes the full SQL compiler for filter expressions over the six search-attribute index tables (`sa_keyword_idx`, `sa_keyword_list_idx`, `sa_int_idx`, `sa_bool_idx`, `sa_datetime_idx`, `sa_double_idx`, `sa_text_token_idx`) with support for `eq`, `in`, `between`, `starts_with`, `ne`, and text-token matching.
  - `VisibilitySink<S>` and `ProjectionWorker` are backend-agnostic; they work against any type that implements `VisibilityStore` or `ProjectionSink`.
  - 377-line integration test at `crates/tokeira-projection/tests/dsql_projection_persistence.rs`.
- `platforms/compose/` crate with `ComposeDeployment`, `ComposeConfig`, `ComposeModule` (runtime + observability variants), `LocalStateModule`, and the existing `collect_writeback` impl in `platforms/compose/src/lib.rs`.
- `tokeirad` uses `mimalloc` as the global allocator and today unconditionally constructs `InMemoryStore::default()` in `apps/tokeirad/src/main.rs`. Branching on an explicit `infrastructure.storage` kind (populated by writeback from the DSQL module) is delivered by this spec — see Feature 8.

### What this spec does NOT cover

- IAM token refresh for DSQL runtime connections — already implemented by the connection-reservoir layer in `tokeira-storage::dsql::connection` + `tokeira_storage::dsql::reservoir`. This spec only wires it into the tokeirad startup path (Feature 8).
- Schema migration tooling internals — already implemented in `tokeira_storage::dsql::migration`.
- The writeback machinery itself — already implemented as `tokeira_iac::write_config_values` and `Deployment::collect_writeback`.
- DSQL connection pool management — already implemented in `tokeira_storage::dsql::connection`.
- Migration SQL files V001–V046 — already present under `crates/tokeira-storage/migrations/` (covers run-repository, projection log, visibility, search-attribute indexes, routing generation, and budget allocation).
- Operator-facing performance tuning of `DsqlPoolConfig` / `ReservoirConfig` / `MigrationConfig` (see "Storage-layer config boundary" below).
- The `[capacity.dsql]` section of `tokeirad.toml` (`max_connections`, `connection_rate_per_second`, `burst_capacity` on `tokeira_config::DsqlCapacityConfig`). This section already exists in the server config model with its own defaults and is operator-tunable; this spec does not wire it through to the storage layer and does not define any writeback into it.

### DSQL defaults appropriate for the compose platform

A compose deployment runs on a developer's laptop or a single CI host. The DSQL settings it exercises are deliberately lighter than what an ECS or EKS deployment would use:

- The tokeirad startup path (Req 8.1) constructs `DsqlPoolConfig::default()` unchanged. The default reservoir size (`target_ready = 50`, `inflight_limit = 8`) is sufficient for a single-replica tokeirad container talking to a DSQL cluster from one host.
- The `DsqlCluster` resource provisions a cluster in single-region managed mode (Req 1.2) rather than any multi-region configuration.
- Writeback populates only `infrastructure.dsql.endpoint` and `infrastructure.dsql.region` (Req 3.1). Role ARNs default to `None`, letting the AWS provider chain resolve caller identity directly — appropriate for local development where the operator is typically authenticated as an IAM user or assumed role that already has `dsql:DbConnectAdmin` permissions.
- The compose service descriptor for `tokeirad` forwards the host's AWS credentials via `~/.aws` mount and the provider-chain environment variables (Req 4.1). No in-container IMDS or workload identity is assumed.

### Storage-layer config boundary

There are two distinct DSQL config surfaces in the workspace and this spec deliberately touches only the first:

1. **Operator-facing infrastructure config** — `TokeiraConfig.infrastructure.dsql` (endpoint, region, role ARNs) populated by this spec's writeback, and `TokeiraConfig.capacity.dsql` which already exists. These are the only DSQL settings a typical operator ever edits.
2. **Storage-internal tuning** — `DsqlPoolConfig`, `ReservoirConfig`, and `MigrationConfig` in `tokeira_storage::dsql::config`. Their source comment explicitly states *"These settings are intentionally internal to the DSQL backend. The server configuration currently exposes only high-level deployment metadata"*. This spec preserves that boundary: the tokeirad startup path in Feature 8 constructs `DsqlPoolConfig::default()` rather than reading any pool-tuning fields from `tokeirad.toml`. A future spec may promote individual fields from storage-internal to operator-facing if and when they need to be tuned per deployment.

## Glossary

- **Compose_Platform**: The `platforms/compose/` crate implementing Docker Compose deployment via `tokeira-orchestrator` traits.
- **ComposeConfig**: The platform configuration struct for compose deployments, loaded from `deployment.toml`.
- **DsqlModule**: A new IaC module in the compose platform that provisions or adopts a DSQL cluster.
- **DsqlCluster**: The existing `tokeira-aws` resource that provisions or adopts an Aurora DSQL cluster.
- **Managed_Mode**: The DsqlCluster lifecycle mode where `tkr infra apply` creates and manages the cluster via AWS APIs.
- **Preexisting_Mode**: The DsqlCluster lifecycle mode where `tkr infra apply` adopts an externally managed cluster endpoint without creating or deleting it.
- **Writeback**: The process of writing infrastructure outputs (DSQL endpoint) back into `tokeirad.toml` after apply.
- **Schema_Setup**: The `tkr schema setup` command that runs DDL migrations against the configured DSQL endpoint.
- **tokeirad_toml**: The server configuration file (`tokeirad.toml`) containing `[infrastructure.dsql]` with the endpoint, region, and role ARNs.

## Requirements

---

## Feature 1: DSQL IaC Module for Compose Platform

### Requirement 1.1: DSQL Module Registration

**User Story:** As a Tokeira operator using the compose platform with DSQL storage, I want `tkr infra apply` to provision or adopt a DSQL cluster, so that my compose deployment has a persistence backend.

#### Acceptance Criteria

1. THE Compose_Platform SHALL define a `DsqlModule` implementing the `Module` trait from `tokeira-iac`.
2. THE `DsqlModule` SHALL have the logical name `"dsql"`.
3. THE `DsqlModule` SHALL declare a dependency on `"local-state"` (the logical name of the compose platform's bootstrap state module — see Feature 9 for the rename).
4. THE `DsqlModule` SHALL be registered in `ComposeDeployment::infra_modules()` when the deployment storage kind is `StorageKind::Dsql`.
5. WHEN the deployment storage kind is `StorageKind::InMemory`, THE Compose_Platform SHALL NOT include the `DsqlModule` in `infra_modules()`.

### Requirement 1.2: Managed Mode Provisioning

**User Story:** As a Tokeira operator, I want `tkr infra apply` to create a new DSQL cluster when no preexisting endpoint is configured, so that I get a fully managed persistence backend with no manual AWS console work.

#### Acceptance Criteria

1. WHEN `ComposeConfig.dsql.mode` is `managed` (or defaulted because no preexisting endpoint is provided), THE `DsqlModule` SHALL include a `DsqlCluster` resource with `DsqlClusterMode::Managed`.
2. WHEN the `DsqlCluster` resource is created in managed mode, THE resource SHALL provision a new Aurora DSQL cluster via the AWS DSQL API.
3. WHEN the managed cluster is provisioned, THE resource state SHALL contain the `cluster_endpoint`, `cluster_arn`, and `cluster_id` properties.
4. THE `DsqlModule` SHALL register `AwsClients` in `ProvisionContext` extensions before resource operations execute.

### Requirement 1.3: Preexisting Mode Adoption

**User Story:** As a Tokeira operator with an existing DSQL cluster, I want to point my compose deployment at that cluster without `tkr infra apply` creating a new one, so that I can share a cluster across environments or use one provisioned by another tool.

#### Acceptance Criteria

1. WHEN `ComposeConfig.dsql.mode` is `preexisting` AND `ComposeConfig.dsql.endpoint` is set, THE `DsqlModule` SHALL include a `DsqlCluster` resource with `DsqlClusterMode::Preexisting`.
2. WHEN the `DsqlCluster` resource is created in preexisting mode, THE resource SHALL record the configured endpoint and ARN in state without calling AWS create APIs.
3. WHEN `tkr infra destroy` is run against a preexisting-mode cluster, THE `DsqlCluster` resource SHALL skip the provider delete call (per the `effective_managed` convention).
4. IF `ComposeConfig.dsql.mode` is `preexisting` AND `ComposeConfig.dsql.endpoint` is empty, THEN THE `DsqlModule` SHALL return a validation error during resource assembly.

### Requirement 1.4: Module Dependency Ordering

**User Story:** As a Tokeira developer, I want the DSQL module to be provisioned after local-state but before observability and runtime, so that the DSQL endpoint is available for writeback before services start.

#### Acceptance Criteria

1. THE `DsqlModule` SHALL declare `["local-state"]` as its module dependencies.
2. FOR DSQL deployments, the full module ordering SHALL be: `local-state` → `dsql` → `observability` → `runtime`. See Req 6.2 for the per-module dependency declarations that produce this ordering.
3. THE `DsqlModule` SHALL NOT be present in the module list for in-memory deployments (Req 1.1.5), so the in-memory ordering remains unchanged.

---

## Feature 2: ComposeConfig DSQL Fields

### Requirement 2.1: DSQL Configuration Section

**User Story:** As a Tokeira operator, I want DSQL settings in my `deployment.toml`, so that I can configure managed vs preexisting mode, endpoint, ARN, and region for my compose deployment.

#### Acceptance Criteria

1. THE `ComposeConfig` SHALL include an optional `dsql` section defined as a `ComposeDsqlConfig` struct with `#[serde(deny_unknown_fields)]` and fields:
   - `mode: DsqlMode` (enum with variants `Managed` and `Preexisting`, default `Managed`)
   - `endpoint: Option<String>` (default `None`)
   - `arn: Option<String>` (default `None`)
   - `region: Option<String>` (default `None` — there is NO built-in default; region resolution at runtime follows the standard AWS provider chain per Req 4.2 and Req 7.1.2, and `us-east-1` is only the last-resort fallback at the `AwsClients` level during cluster provisioning)
2. THE `ComposeConfig` SHALL use `serde(deny_unknown_fields)` on the DSQL config section to reject typos at parse time.
3. WHEN `ComposeConfig.dsql` is `None` (section absent) AND storage is `StorageKind::Dsql`, THE Compose_Platform SHALL treat this as managed mode with defaults.
4. FOR ALL valid `ComposeConfig` values with DSQL fields, serializing to TOML and deserializing back SHALL produce an equivalent `ComposeConfig` (round-trip property).

### Requirement 2.2: Storage Kind Awareness

**User Story:** As a Tokeira developer, I want `ComposeConfig` to carry the storage kind, so that module assembly and service configuration can branch on whether DSQL is active.

#### Acceptance Criteria

1. THE `ComposeConfig` SHALL include a `storage` field of type `StorageKind` (default `InMemory`).
2. WHEN `tkr deployment create --platform compose --storage dsql` is run, THE generated `deployment.toml` SHALL include `storage = "dsql"` and a `[dsql]` section with `mode = "managed"` and empty `endpoint` / `arn` fields (placeholders for operator override or writeback). The `region` field SHALL NOT be included — region resolution follows the standard AWS provider chain (Req 4.2). If the operator wants to override the region, they add `region = "eu-west-1"` manually.
3. WHEN `tkr deployment create --platform compose --storage in-memory` is run, THE generated `deployment.toml` SHALL NOT include a `[dsql]` section.

---

## Feature 3: DSQL Endpoint Writeback

### Requirement 3.1: Writeback After Infra Apply

**User Story:** As a Tokeira operator, I want the DSQL endpoint and the storage kind written to `tokeirad.toml` automatically after `tkr infra apply`, so that `tkr schema setup` and `tkr deploy apply` can find the endpoint without manual configuration and tokeirad knows to open a DSQL-backed store.

#### Acceptance Criteria

1. WHEN `tkr infra apply` completes successfully with a DSQL module, THE `ComposeDeployment::collect_writeback()` SHALL return:
   - `("infrastructure.storage", "dsql")` — the authoritative storage-kind signal tokeirad reads on startup (see Feature 8).
   - `("infrastructure.dsql.endpoint", <endpoint_value>)` — the resolved DSQL cluster endpoint.
   - `("infrastructure.dsql.region", <region_value>)` — only when the DSQL cluster state contains a `region` property or the config specifies a region.
2. WHEN the DSQL cluster state contains a `cluster_endpoint` property, THE writeback SHALL use that value for `infrastructure.dsql.endpoint`.
3. THE writeback SHALL set `infrastructure.storage = "dsql"` whenever it writes `infrastructure.dsql.endpoint`. The two values are written atomically via a single `write_config_values(path, values)` call so tokeirad cannot observe an intermediate state where the endpoint is set but the storage kind is not.
4. WHEN the DSQL module is not present in state (in-memory storage), THE `collect_writeback()` SHALL return an empty vector (existing behaviour). It SHALL NOT write `infrastructure.storage = "in-memory"` — the field's default in the TOML model is `InMemory`, so absence is equivalent and writeback stays no-op for in-memory deployments.

### Requirement 3.2: Writeback Idempotence

**User Story:** As a Tokeira operator, I want repeated `tkr infra apply` runs to produce the same `tokeirad.toml` content, so that the writeback is safe to run multiple times.

#### Acceptance Criteria

1. FOR ALL infra apply operations where neither the DSQL endpoint nor the storage kind has changed, THE writeback SHALL overwrite existing values with the same values (no-op in practice).
2. WHEN the DSQL endpoint changes (e.g., switching from managed to a different preexisting cluster), THE writeback SHALL update `tokeirad.toml` with the new endpoint while keeping `infrastructure.storage = "dsql"`.
3. IF an operator manually sets `infrastructure.storage = "in-memory"` in `tokeirad.toml` and then runs `tkr infra apply` with the DSQL module active, THE writeback SHALL overwrite that value with `"dsql"`. The operator's `deployment.toml` + `infra apply` pair is the authority; hand-edits to `tokeirad.toml` are not preserved across infra-apply runs.

---

## Feature 4: AWS Credentials in Compose Containers

### Requirement 4.1: AWS Credentials for the Standard Provider Chain

**User Story:** As a Tokeira operator running compose+dsql locally, I want the `tokeirad` container to have access to my AWS credentials via the standard credential provider chain, so that IAM authentication to DSQL works at runtime using the same credential resolution as the AWS CLI on my host.

#### Acceptance Criteria

1. WHEN storage is `StorageKind::Dsql`, THE compose service descriptor for `tokeirad` SHALL mount the host's AWS configuration into the container so that the standard AWS credential provider chain works correctly inside the container. This means:
   - Mount `~/.aws` (host) to the container's expected AWS config directory as read-only.
   - If `AWS_SHARED_CREDENTIALS_FILE` or `AWS_CONFIG_FILE` environment variables are set on the host, mount those specific paths instead of `~/.aws`.
   - Forward the following environment variables from the host into the container when set: `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_SHARED_CREDENTIALS_FILE`, `AWS_CONFIG_FILE`. This ensures SSO sessions, assumed roles, and environment-variable credentials all work.
2. THE mount target inside the container SHALL match the user the `tokeirad` image runs as. The current `tokeirad` image uses the Chainguard `glibc-dynamic` base and runs as `nonroot` at UID 65532, so the mount target SHALL be `/home/nonroot/.aws`. If a future image change moves the container to a different user, the mount target SHALL follow. The `HOME` environment variable inside the container SHALL be set consistently with the mount target.
3. WHEN storage is `StorageKind::InMemory`, THE compose service descriptor for `tokeirad` SHALL NOT include any AWS credential mounts or environment variable forwarding.
4. THE credential mounting SHALL NOT copy or persist credentials into the container image or any writable layer — read-only bind mount only.

### Requirement 4.2: AWS Region Resolution

**User Story:** As a Tokeira operator, I want the AWS region resolved from my credentials/config (the same way the AWS CLI resolves it), so that I don't have to specify it separately unless I want to override.

#### Acceptance Criteria

1. WHEN storage is `StorageKind::Dsql` AND `ComposeConfig.dsql.region` is explicitly set, THE compose service descriptor for `tokeirad` SHALL include an `AWS_REGION` environment variable set to that configured value.
2. WHEN `ComposeConfig.dsql.region` is NOT set, THE compose service descriptor SHALL NOT set `AWS_REGION` explicitly — allowing the standard provider chain inside the container to resolve the region from `~/.aws/config` (the `[profile ...]` region setting) or from `AWS_DEFAULT_REGION` forwarded from the host.
3. WHEN neither the config nor the provider chain yields a region, THE `DsqlModule` SHALL default to `us-east-1` during cluster provisioning (this is the `tkr infra apply` path, not the container runtime path — the container relies on the provider chain or the explicit config override).

---

## Feature 5: Schema Setup Wiring

### Requirement 5.1: Schema Setup Execution

**User Story:** As a Tokeira operator, I want `tkr schema setup` to execute DDL migrations against the DSQL endpoint after `tkr infra apply`, so that the database schema is ready before services start.

#### Acceptance Criteria

1. WHEN `tkr schema setup` is run for a compose+dsql deployment, THE CLI SHALL read the DSQL endpoint from `tokeirad.toml` at `infrastructure.dsql.endpoint`.
2. WHEN the endpoint is configured, THE CLI SHALL:
   a. Construct a `PgPool` connection to the endpoint using `aurora-dsql-sqlx-connector` for IAM authentication. Region is resolved from the endpoint hostname via `tokeira_storage::dsql::config::detect_region_from_endpoint` (matches `*.dsql.{region}.on.aws`) or from the config's `infrastructure.dsql.region` field when the hostname pattern does not yield a region.
   b. Construct a `tokeira_storage::dsql::MigrationRunner::new(MigrationConfig { migrations_dir })` where `migrations_dir` points to the embedded migration files from `crates/tokeira-storage/migrations/` (V001–V046).
   c. Call `runner.apply(&pool).await?` which applies all unapplied migrations in strict version order, one DDL statement per transaction, with checksum verification.
   d. Report the `MigrationReport { applied: N }` to the operator: "Applied N migration(s) to {endpoint}".
3. IF the endpoint is not configured in `tokeirad.toml`, THEN THE CLI SHALL return an error: "dsql endpoint is not configured in {path}; run `tkr infra apply --module dsql` first".
4. THE `tkr schema setup` command SHALL require `--yes` or interactive confirmation before executing migrations.
5. THE migration files (V001–V046) SHALL be embedded in the `tkr` binary at compile time (via `include_str!` or a build-time directory embed) so that `tkr schema setup` does not require the source tree to be present at runtime. Alternatively, `MigrationConfig.migrations_dir` can point to a path resolved relative to the workspace root if the operator always runs from a checkout — document which approach is chosen.

### Requirement 5.2: Schema Status Check

**User Story:** As a Tokeira operator, I want `tkr schema status` to report whether the DSQL schema is up to date, so that I can verify readiness before starting services.

#### Acceptance Criteria

1. WHEN `tkr schema status` is run for a compose+dsql deployment, THE CLI SHALL connect to the DSQL endpoint (same connection path as 5.1.2.a) and call `runner.status(&pool).await?`.
2. THE CLI SHALL report the `SchemaStatus { current_version: Option<u32>, checked_at }`: "Schema version: V{version:03} (checked at {time})" or "Schema not initialized (no schema_version table)" when `current_version` is `None`.
3. IF the endpoint is not configured, THE CLI SHALL return the same error as Requirement 5.1.3.

### Requirement 5.3: Schema Validation (offline)

**User Story:** As a Tokeira developer, I want `tkr schema validate` to check migration files against DSQL constraints without connecting to a cluster, so that I can catch schema mistakes before they reach production.

#### Acceptance Criteria

1. THE CLI SHALL expose a `tkr schema validate` subcommand that calls `runner.validate()` (no pool required — purely local/static analysis).
2. THE subcommand SHALL report any `ValidationIssue` entries: file, line, kind (e.g., `Bigserial`, `Check`, `TempTable`, `PlPgsql`, `MissingAsync`), and message.
3. WHEN no issues are found, THE subcommand SHALL print "All migrations valid" and exit 0.
4. WHEN issues are found, THE subcommand SHALL print each issue and exit 1.

---

## Feature 6: Lifecycle Ordering

### Requirement 6.1: Compose+DSQL Deployment Lifecycle

**User Story:** As a Tokeira operator, I want a clear lifecycle ordering for compose+dsql deployments, so that each step has its prerequisites satisfied and tokeirad starts against a fully-migrated database with observability already capturing its startup.

#### Acceptance Criteria

1. THE compose+dsql lifecycle SHALL follow this order:
   - `tkr deployment create --name <depname> --platform compose --storage dsql` — creates the deployment directory and writes an initial `deployment.toml` with `storage = "dsql"` and a `[dsql]` section containing `mode = "managed"` and empty `endpoint` / `arn` fields (Req 2.2.2), plus an initial `tokeirad.toml` with `[infrastructure.dsql]` present but `endpoint` unset
   - `tkr image build` — produces `tokeirad:latest` in the local Docker image store (see "Upstream dependencies")
   - `tkr infra apply --module dsql` — provisions/adopts the DSQL cluster, writes endpoint to `tokeirad.toml`
   - `tkr schema setup --yes` — runs migrations against the endpoint (requires the endpoint to be present in `tokeirad.toml`)
   - `tkr infra apply` — provisions remaining modules (observability, runtime) now that the database is ready
   - `tkr deploy apply --yes` — starts tokeirad with DSQL persistence and the observability stack
2. THE two-phase infra apply is deliberate: the DSQL cluster must exist and be schema-ready before the runtime module's compose services are created, because `tokeirad` connects to DSQL on startup and will crash-loop if the schema is missing. Running `--module dsql` first isolates the storage provisioning from the service provisioning.
3. IF `tkr deploy apply` is run before `tkr image build`, THE compose platform's pre-deploy gate (the `validate_for_deploy_apply` / local-image existence check provided by the upstream image-lifecycle work) SHALL refuse to proceed with the "run `tkr image build` first" error.
4. IF `tkr deploy apply` is run before `tkr schema setup`, THE `tokeirad` service SHALL fail to start with a clear error from the DSQL storage layer (schema tables missing). THE CLI SHALL NOT enforce this ordering — the operator is responsible for following the documented lifecycle.
5. IF `tkr schema setup` is run before `tkr infra apply --module dsql` (no endpoint in `tokeirad.toml`), THEN THE CLI SHALL return the error from Requirement 5.1.3.
6. A SINGLE `tkr infra apply` (without `--module`) SHALL also work as a one-shot alternative to the two-phase approach — it provisions all modules in dependency order (`local-state` → `dsql` → `observability` → `runtime`). The operator then runs `tkr schema setup` after the full apply and before `tkr deploy apply`. The two-phase approach is the RECOMMENDED workflow because it lets schema setup run before observability/runtime containers are created; the one-shot approach is acceptable when the operator is comfortable with tokeirad crash-looping briefly until schema setup completes.
7. THE README documentation SHALL include both the recommended two-phase workflow and the one-shot alternative.

### Requirement 6.2: Module Dependencies Reflect Storage Kind

**User Story:** As a Tokeira developer, I want the module dependency graph to adapt based on storage kind, so that DSQL provisioning is ordered correctly without affecting in-memory deployments.

#### Acceptance Criteria

1. FOR IN-MEMORY deployments, the module dependency graph SHALL remain unchanged: `local-state` → `runtime` → `observability` (existing behaviour post-rename).
2. FOR DSQL deployments, the module dependency graph SHALL be: `local-state` → `dsql` → `observability` → `runtime`. This reverses the runtime/observability order compared to in-memory because:
   - `dsql` must come before observability so the endpoint is written back before any service that might reference it.
   - `runtime` (tokeirad) depends on observability being ready (Alloy sidecar, Mimir endpoint) AND on the DSQL endpoint being configured.
3. WHEN storage is `StorageKind::Dsql`:
   - `DsqlModule` dependencies: `["local-state"]`
   - `observability` ComposeModule dependencies: `["local-state", "dsql"]`
   - `runtime` ComposeModule dependencies: `["observability"]`
4. WHEN storage is `StorageKind::InMemory`:
   - `runtime` ComposeModule dependencies: `["local-state"]`
   - `observability` ComposeModule dependencies: `["local-state", "runtime"]`
5. THE `ComposeModule::dependencies()` method SHALL accept the storage kind as context (either via a field on the module struct or via a constructor parameter) so it can return the correct dependency list per mode.

---

## Feature 7: ProvisionContext Extension Registration

### Requirement 7.1: AWS Clients Extension for Compose+DSQL

**User Story:** As a Tokeira developer, I want `AwsClients` registered in `ProvisionContext` when the compose platform uses DSQL, so that the `DsqlCluster` resource can call AWS APIs during create/update/delete/describe.

#### Acceptance Criteria

1. WHEN storage is `StorageKind::Dsql`, THE `ComposeDeployment::register_infra_extensions()` SHALL construct an `AwsClients` instance and register it in `ProvisionContext` via `set_extension()`.
2. THE `AwsClients` instance SHALL resolve its region using the standard AWS provider chain: first `ComposeConfig.dsql.region` (explicit override), then the environment (`AWS_REGION` / `AWS_DEFAULT_REGION`), then `~/.aws/config` profile region, then fall back to `us-east-1` as the last resort. This matches how the AWS SDK resolves region by default — the implementation should use `aws_config::load_defaults(BehaviorVersion::latest()).await` with an optional region override from config.
3. WHEN storage is `StorageKind::InMemory`, THE `register_infra_extensions()` SHALL NOT register `AwsClients` (existing no-op behavior).
4. IF AWS credential resolution fails during extension registration, THEN THE method SHALL return a descriptive error indicating that AWS credentials are required for DSQL storage and suggesting the operator check `aws configure` or their environment variables.

---

## Feature 8: Tokeirad Storage Backend Selection

### Requirement 8.1: Explicit Storage-Kind Field in Server Config

**User Story:** As a Tokeira developer, I want `TokeiraConfig.infrastructure.storage` to carry an explicit storage-kind selector, so that tokeirad's runtime backend choice is driven by operator intent rather than inferred from field presence.

#### Acceptance Criteria

1. THE `tokeira-config` crate SHALL add a new enum type `ConfigStorageKind` with variants `InMemory` and `Dsql`, `#[serde(rename_all = "kebab-case")]`, with `InMemory` as the `#[default]` variant. The name `ConfigStorageKind` distinguishes this server-config-level type from `tokeira_orchestrator::StorageKind`, which is a deployment-layer CLI concern; the two enums are structurally identical by design and a helper `From` conversion MAY be provided for call-site ergonomics.
2. THE `InfrastructureConfig` struct SHALL gain a `storage: ConfigStorageKind` field with `#[serde(default)]`, placed alongside the existing `dsql: DsqlInfraConfig` sub-section.
3. WHEN `tokeirad.toml` does not set `infrastructure.storage`, THE loaded `TokeiraConfig` SHALL report `infrastructure.storage == ConfigStorageKind::InMemory` (the default).
4. THE `TokeiraConfig::validate` function SHALL reject the combination `infrastructure.storage == ConfigStorageKind::Dsql` AND `infrastructure.dsql.endpoint.is_none()` (or empty string) with a `ValidationError::Field { field: "infrastructure.dsql.endpoint", message: "must be set when infrastructure.storage is dsql; run `tkr infra apply --module dsql` first" }`. This fails closed at config-load time, before tokeirad tries to open any sockets.
5. THE existing `#[serde(deny_unknown_fields)]` attribute on `InfrastructureConfig` SHALL be preserved — adding a new field does not soften the rejection of typos.
6. FOR ALL `TokeiraConfig` values with `infrastructure.storage` set, serializing to TOML and deserializing back SHALL produce an equivalent `TokeiraConfig` (round-trip property, asserted by the existing `toml_round_trip_preserves_config` test extended with the new field).

### Requirement 8.2: Runtime Storage Backend Branches on `infrastructure.storage`

**User Story:** As a Tokeira operator running compose+dsql, I want `tokeirad` to construct a DSQL-backed store when `infrastructure.storage = "dsql"` is set in `tokeirad.toml`, so that compose+dsql deployments actually persist to DSQL.

#### Acceptance Criteria

1. WHEN `tokeirad` starts AND `TokeiraConfig.infrastructure.storage == ConfigStorageKind::Dsql`, THE startup path in `apps/tokeirad/src/main.rs` SHALL construct a DSQL-backed `RunRepository` instead of the `InMemoryStore` currently hard-coded there.
2. THE DSQL-backed storage SHALL be assembled by calling the existing public facade `tokeira_storage::dsql::DsqlStore::connect(auth, config)` in `crates/tokeira-storage/src/dsql/mod.rs`, which already wires a shared `Arc<DsqlConnectionDirector>`, a `DsqlRunRepository`, a `DsqlProjectionLog`, and a `MigrationRunner` over one coordinated reservoir:
   - A `DsqlAuthConfig` built by copying `endpoint`, `region`, `admin_role_arn`, `runtime_role_arn`, and `readonly_role_arn` from `TokeiraConfig.infrastructure.dsql`.
   - A `DsqlPoolConfig::default()` (per the "Storage-layer config boundary" note; pool tuning is not operator-facing).
   - The `DsqlStore::run_repository()` accessor returns the `&DsqlRunRepository` that tokeirad wraps in `HistoryNotifyingRepository`.
   - The `DsqlStore::projection_log()` accessor returns the `&DsqlProjectionLog` that each `ProjectionWorker` reads from.
   - The same `Arc<DsqlConnectionDirector>` (exposed via `DsqlStore::connection_director()`) SHALL be cloned into the `DsqlVisibilityStore` constructed under Req 8.4 so the visibility sink, repository writes, and projection-log reads share one reservoir and class-budget state.
3. WHEN `TokeiraConfig.infrastructure.storage == ConfigStorageKind::InMemory`, THE startup path SHALL continue to use `InMemoryStore::default()` (existing behaviour).
4. WHEN the DSQL-backed storage fails to connect at startup (invalid endpoint, IAM auth failure, DNS failure, schema tables missing), THE process SHALL exit with a non-zero status and log the error via the existing `tracing` infrastructure. It SHALL NOT silently fall back to `InMemoryStore`. This failure is distinct from Req 8.1.4: Req 8.1.4 fires at config-load time before any network I/O; Req 8.2.4 fires during connection establishment.
5. THE DSQL-backed run repository SHALL be wrapped by `HistoryNotifyingRepository` the same way `InMemoryStore` is wrapped today — only the inner `Arc<dyn RunRepository>` differs.
6. THE projection workers and visibility query service SHALL use the DSQL-backed `DsqlVisibilityStore` under DSQL storage — see Req 8.4. No in-memory fallback remains for the visibility path when `infrastructure.storage == Dsql`.

### Requirement 8.3: Storage Backend Observability

**User Story:** As a Tokeira operator, I want `tokeirad` to log the active storage backend at startup, so that I can quickly confirm whether my configuration took effect.

#### Acceptance Criteria

1. ON startup, `tokeirad` SHALL log an `INFO`-level message identifying the active storage backend: either `"storage backend: in-memory"` or `"storage backend: dsql (endpoint={endpoint})"` where `{endpoint}` is the configured DSQL endpoint.
2. THE log message SHALL NOT include IAM role ARNs, credentials, or any other sensitive values.
3. THE log message SHALL appear before the gRPC listener binds so operators can see the selection even if subsequent startup fails.

### Requirement 8.4: DSQL-Backed Visibility and Projection Wiring

**User Story:** As a Tokeira operator running compose+dsql, I want workflow visibility and projection state to persist in DSQL the same way run history does, so that List/Count visibility queries survive tokeirad restarts and so projection checkpoints are durable.

#### Acceptance Criteria

1. WHEN `infrastructure.storage == ConfigStorageKind::Dsql`, THE startup path SHALL construct a `tokeira_projection::dsql_store::DsqlVisibilityStore::new(director)` using the same `Arc<DsqlConnectionDirector>` obtained from `DsqlStore::connection_director()`. The `DsqlVisibilityStore` already exists in `crates/tokeira-projection/src/dsql_store.rs` (gated by the `dsql` feature on `tokeira-projection`) and implements both `VisibilityStore` (for query reads) and `ProjectionSink` (for projection-log consumption).
2. THE `tokeira-projection` crate dependency in `apps/tokeirad/Cargo.toml` SHALL enable the `dsql` feature so that `DsqlVisibilityStore` is accessible. The dependency SHALL be written as `tokeira-projection = { path = "...", features = ["dsql"] }`.
3. THE `VisibilityQueryService` SHALL be constructed over the `DsqlVisibilityStore` (replacing today's unconditional `VisibilityQueryService::new(InMemoryVisibilityStore::default())` at `apps/tokeirad/src/main.rs` line 216–217).
4. THE projection workers spawned in `apps/tokeirad/src/main.rs` SHALL read from `DsqlStore::projection_log()` (a `&DsqlProjectionLog` — implements the `ProjectionLog` trait from `tokeira-storage`) and write to `DsqlVisibilityStore` (via its `ProjectionSink` impl) instead of the current `InMemoryStore` / `VisibilitySink<InMemoryVisibilityStore>` pair. Partition count, batch size, and cursor-from-beginning semantics SHALL be preserved.
5. THE `DsqlVisibilityStore` and `DsqlProjectionLog` SHALL share the single `Arc<DsqlConnectionDirector>` held by `DsqlStore` — tokeirad SHALL NOT construct a second director. Sharing the director is the mechanism that keeps reservoir capacity, class budgets, and rate limits globally coordinated across repository writes, projection-log reads, and visibility sink writes.
6. WHEN `infrastructure.storage == ConfigStorageKind::InMemory`, the visibility and projection wiring SHALL remain unchanged: `VisibilityQueryService::new(InMemoryVisibilityStore::default())` with `ProjectionWorker { log: InMemoryStore, sink: VisibilitySink<InMemoryVisibilityStore> }` per partition (existing behaviour).
7. THE schema migrations required by `DsqlVisibilityStore` (tables `vis_execution`, `vis_rollup`, `sa_registry`, `sa_current`, and the six `sa_*_idx` indexes covered by migration files V012, V017, V029–V042) SHALL be applied by the same `tkr schema setup` step that Feature 5 defines. No separate visibility-only schema-setup command is needed because `MigrationRunner::apply` runs all unapplied migrations in strict version order.

---

## Feature 9: Compose State Module Rename

### Requirement 9.1: Rename Compose Platform State Module to `"local-state"`

**User Story:** As a Tokeira developer, I want the compose platform's bootstrap state module to be called `"local-state"` rather than `"remote-state"`, so that the name reflects what it actually does — manage a local state directory on the developer's host.

#### Acceptance Criteria

1. THE `LocalStateModule::name()` method in `platforms/compose/src/modules.rs` SHALL return `"local-state"` (currently returns `"remote-state"`).
2. THE `LocalStateResource::module()` method in the same file SHALL return `"local-state"`.
3. THE `module` field on the `ResourceState` returned by `LocalStateResource::create()` and `LocalStateResource::describe()` SHALL be set to `"local-state"`.
4. THE `ComposeModule::dependencies()` match arms in `platforms/compose/src/modules.rs` SHALL declare `["local-state"]` for the runtime module and `["local-state", "runtime"]` for the observability module in the in-memory storage case, matching Req 6.2.4 post-rename.
5. THE comment banner `// ── Local state module (remote-state bootstrap) ───────────────────` in `platforms/compose/src/modules.rs` SHALL be updated to remove the parenthetical, since the rename eliminates the need to bridge between the trait method name and the logical name.
6. THE scope of this rename SHALL be limited to the compose platform (`platforms/compose/`). The ECS platform's module name and the `Deployment::remote_state_module` trait method in `tokeira-orchestrator` SHALL NOT be changed by this spec — those require a wider alignment across platforms that is outside this spec's scope.
7. IF a test asserts on the compose platform's state module name, that test SHALL be updated to expect `"local-state"`.

### Requirement 9.2: Trait Method Naming Preserved

**User Story:** As a Tokeira developer, I want the `Deployment::remote_state_module` trait method to keep its current name, so that compose, ecs, and local platforms continue to share the same trait contract without a cross-platform refactor.

#### Acceptance Criteria

1. THE `Deployment::remote_state_module` trait method in `tokeira-orchestrator` SHALL remain named `remote_state_module` — the compose platform's implementation just returns a module whose logical name is `"local-state"`.
2. A doc-comment on the trait method SHALL be updated to note that each platform picks its own logical module name (e.g., `"local-state"` for compose, whatever ECS uses today) and that the trait method name predates the per-platform naming decision.
3. A workspace-wide rename of the trait method itself is explicitly deferred to a future spec.
