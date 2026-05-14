# Implementation Plan: Compose DSQL Persistence

## Overview

This plan integrates Aurora DSQL as a storage backend for the compose platform by wiring existing crates (`tokeira-aws`, `tokeira-storage`, `tokeira-projection`) into the compose platform's IaC module system, config model, service descriptors, and tokeirad startup path. Additional tasks cover dashboard improvements, documentation updates, and code documentation.

## Tasks

- [x] 1. Rename state module and update ComposeConfig model
  - [x] 1.1 Rename `LocalStateModule` logical name from `"remote-state"` to `"local-state"`
    - In `platforms/compose/src/modules.rs`, change `LocalStateModule::name()` to return `"local-state"`
    - Update `LocalStateResource::module()` to return `"local-state"`
    - Update `ResourceState.module` in `create()` and `describe()` to `"local-state"`
    - Update the section comment banner to remove the `(remote-state bootstrap)` parenthetical
    - Update all tests that assert on the module name
    - _Requirements: 9.1.1, 9.1.2, 9.1.3, 9.1.5, 9.1.7_

  - [x] 1.2 Add `ComposeDsqlConfig`, `DsqlMode`, and `StorageKind` fields to `ComposeConfig`
    - Add `ComposeDsqlConfig` struct with `#[derive(Default)]` and `#[serde(deny_unknown_fields)]` and fields: `mode: DsqlMode`, `endpoint: Option<String>`, `arn: Option<String>`, `region: String` (default `"us-east-1"` via `#[serde(default = "default_region")]`; the `Default` impl uses the same value)
    - Add `DsqlMode` enum with `Managed` (default) and `Preexisting` variants, `#[serde(rename_all = "lowercase")]`
    - Add `storage: StorageKind` field to `ComposeConfig` with `#[serde(default = "default_storage_kind")]` and a helper `fn default_storage_kind() -> StorageKind { StorageKind::InMemory }` (because `StorageKind` does not derive `Default`)
    - Add `dsql: Option<ComposeDsqlConfig>` field to `ComposeConfig`
    - Region is always explicit — set at `tkr deployment create --region <region>` time, defaults to `us-east-1`. No endpoint-based region discovery.
    - Add doc comments on `ComposeDsqlConfig`, `DsqlMode`, and the new `ComposeConfig` fields explaining their purpose and the managed/preexisting distinction
    - _Requirements: 2.1.1, 2.1.2, 2.1.3, 2.2.1_

  - [ ]* 1.3 Write property test for ComposeConfig TOML round-trip (Property 2)
    - **Property 2: ComposeConfig TOML round-trip**
    - Generate arbitrary `ComposeConfig` values including DSQL fields and storage kind
    - Assert serialize-then-deserialize produces equivalent config
    - **Validates: Requirements 2.1.4**

- [x] 2. Implement DsqlModule and storage-aware module dependencies
  - [x] 2.1 Add AWS and IaC dependencies to `platforms/compose/Cargo.toml`
    - Add `tokeira-aws` dependency (for `DsqlCluster`, `AwsClients`)
    - Add `aws-config` and `aws-sdk-sts` dependencies (for SDK config loading and credential validation)
    - These are required by `DsqlModule::resources()` and `register_infra_extensions()`
    - _Requirements: 1.2.1, 7.1.1_

  - [x] 2.2 Implement `DsqlModule` struct with `Module` trait
    - Create `DsqlModule` in `platforms/compose/src/modules.rs` with fields: `config: ComposeDsqlConfig`, `project_name: String`
    - Implement `name() -> "dsql"`, `dependencies() -> &["local-state"]`
    - Implement `resources()` to:
      - Validate preexisting mode has a non-empty endpoint; return `IacError::Other` if missing
      - Construct a `tokeira_aws::ResourceContext { project: self.project_name, region: self.config.region, tags }` with default `ManagedBy: tkr` tag
      - Construct `DsqlCluster::new("{project_name}-compose", cluster_config, &rctx)` with mode from `ComposeDsqlConfig.mode`
    - Add doc comments on `DsqlModule` explaining it provisions or adopts a DSQL cluster for compose deployments
    - _Requirements: 1.1.1, 1.1.2, 1.1.3, 1.2.1, 1.3.1, 1.3.4, 1.4.1_

  - [x] 2.3 Make `ComposeModule::dependencies()` storage-kind-aware
    - Add a `storage: StorageKind` field to `ComposeModule`
    - Update `ComposeModule::runtime()` and `ComposeModule::observability()` constructors to accept storage kind
    - Implement conditional dependency logic per the design: DSQL mode reverses runtime/observability ordering
    - Add inline comments explaining WHY the ordering reverses for DSQL (endpoint must be written back before services start)
    - _Requirements: 6.2.1, 6.2.2, 6.2.3, 6.2.4, 6.2.5_

  - [x] 2.4 Register `DsqlModule` in `ComposeDeployment::infra_modules()` conditionally
    - When `config.storage == StorageKind::Dsql`, include `DsqlModule` in the returned modules
    - When `config.storage == StorageKind::InMemory`, exclude `DsqlModule`
    - Pass storage kind to `ComposeModule::runtime()` and `ComposeModule::observability()`
    - _Requirements: 1.1.4, 1.1.5_

  - [ ]* 2.5 Write property test for storage-kind-conditional module inclusion (Property 1)
    - **Property 1: Storage-kind-conditional module inclusion**
    - Generate arbitrary configs with both storage kinds
    - Assert DSQL configs produce a `"dsql"` module; InMemory configs do not
    - **Validates: Requirements 1.1.4, 1.1.5**

  - [ ]* 2.6 Write property test for module dependency graph correctness (Property 7)
    - **Property 7: Module dependency graph correctness per storage kind**
    - Assert InMemory ordering: `local-state` → `runtime` → `observability`
    - Assert DSQL ordering: `local-state` → `dsql` → `observability` → `runtime`
    - **Validates: Requirements 6.2.1, 6.2.2**

  - [ ]* 2.7 Write property test for preexisting mode validation (Property 10)
    - **Property 10: Preexisting mode validation rejects empty endpoint**
    - Generate `ComposeDsqlConfig` with `mode == Preexisting` and empty/None endpoint
    - Assert `DsqlModule::resources()` returns an error
    - **Validates: Requirements 1.3.4**

- [ ] 3. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement writeback and AWS credential mounting
  - [x] 4.1 Implement `ComposeDeployment::collect_writeback()` for DSQL
    - When `config.storage == Dsql`, read DSQL resource state from `InfraState` and return writeback pairs: `infrastructure.storage`, `infrastructure.dsql.endpoint`, and `infrastructure.dsql.region`
    - Region writeback: always write `infrastructure.dsql.region` from `ComposeConfig.dsql.region` (which is set at `tkr deployment create --region <region>` time). There is NO region discovery from the DSQL endpoint hostname — region is always explicit in config.
    - When `config.storage == InMemory`, return empty vector (existing behaviour)
    - Add doc comment explaining the writeback atomicity guarantee (all keys written in one `write_config_values` call) and that region is always explicit (no endpoint-based inference)
    - _Requirements: 3.1.1, 3.1.2, 3.1.3, 3.1.4, 3.2.1, 3.2.2, 3.2.3_

  - [x] 4.2 Add AWS credential mounting to `tokeirad` compose service descriptor
    - In `platforms/compose/src/compose.rs`, when `storage == StorageKind::Dsql`:
      - Add `~/.aws:/home/nonroot/.aws:ro` volume mount
      - Set `HOME=/home/nonroot` environment variable
      - Forward simple AWS provider-chain env vars from host when set: `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_ROLE_ARN`
      - Do NOT forward `AWS_SHARED_CREDENTIALS_FILE` or `AWS_CONFIG_FILE` — these contain host-specific paths that won't resolve inside the container. The `~/.aws` mount provides the standard credential/config files at the expected container path.
      - Do NOT forward `AWS_WEB_IDENTITY_TOKEN_FILE` — the token file path is host-specific and would require a separate bind mount. Web identity auth is not a supported compose+dsql credential method.
      - Set `AWS_REGION` to `ComposeDsqlConfig.region` (always explicit, defaults to `us-east-1`)
    - Add inline comments explaining WHY the `~/.aws` mount targets `/home/nonroot/.aws` (Chainguard base image runs as UID 65532 `nonroot`) and WHY custom file env vars are excluded (host paths don't resolve in container)
    - When `storage == InMemory`, no AWS mounts or env vars
    - _Requirements: 4.1.1, 4.1.2, 4.1.3, 4.1.4, 4.2.1, 4.2.2_

  - [ ]* 4.3 Write property test for writeback correctness (Property 3)
    - **Property 3: Writeback produces correct keys for DSQL state**
    - Generate `InfraState` with/without DSQL cluster resource
    - Assert correct writeback pairs or empty vector
    - **Validates: Requirements 3.1.1, 3.1.2, 3.1.4**

  - [ ]* 4.4 Write property test for AWS credential mounting (Property 5)
    - **Property 5: AWS credential mounting conditional on storage kind**
    - Generate configs with both storage kinds
    - Assert DSQL configs produce `~/.aws` mount and `HOME` env; InMemory does not
    - **Validates: Requirements 4.1.1, 4.1.3**

  - [ ]* 4.5 Write property test for AWS_REGION wiring (Property 6)
    - **Property 6: AWS_REGION env var follows explicit DSQL region**
    - Generate configs with both storage kinds and arbitrary valid DSQL regions
    - Assert DSQL configs always set `AWS_REGION` to `config.dsql.region`; InMemory configs do not set `AWS_REGION`
    - **Validates: Requirements 4.2.1, 4.2.2**

- [x] 5. Implement ProvisionContext extension registration
  - [x] 5.1 Register `AwsClients` in `register_infra_extensions()` for DSQL
    - When `config.storage == StorageKind::Dsql`, load AWS SDK config using the region from `ComposeDsqlConfig.region` (always explicit, defaults to `us-east-1`). Use `aws_config::defaults(BehaviorVersion::latest()).region(Region::new(config_region)).load().await`
    - Construct `AwsClients::new(&sdk_config)` and register via `ctx.set_extension()`
    - After constructing `AwsClients`, call STS `GetCallerIdentity` to eagerly validate credentials. If this fails, return a descriptive error: "AWS credentials required for DSQL storage; check `aws configure` or environment variables"
    - Add doc comment explaining WHY AwsClients is registered conditionally (DsqlCluster resource needs it for AWS API calls) and WHY we eagerly validate (credential resolution is lazy in the SDK; surfacing failures here gives a clear error before the plan/apply cycle begins)
    - _Requirements: 7.1.1, 7.1.2, 7.1.3, 7.1.4_

- [ ] 6. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Implement ConfigStorageKind and tokeirad startup branching
  - [x] 7.1 Add `ConfigStorageKind` enum to `tokeira-config`
    - Add `ConfigStorageKind` with `InMemory` (default) and `Dsql` variants, `#[serde(rename_all = "kebab-case")]`
    - Add `storage: ConfigStorageKind` field with `#[serde(default)]` to `InfrastructureConfig`
    - Add validation rule: reject `Dsql` + missing/empty endpoint with `ValidationError::Field`
    - Add doc comment on `ConfigStorageKind` explaining it is the server-config-level storage selector, distinct from `tokeira_orchestrator::StorageKind`
    - _Requirements: 8.1.1, 8.1.2, 8.1.3, 8.1.4, 8.1.5_

  - [x] 7.2 Add `Arc`-returning accessors to `DsqlStore` facade
    - Add `pub fn connection_director_arc(&self) -> Arc<DsqlConnectionDirector>` that clones the internal `Arc`
    - Add `pub fn into_parts(self) -> (Arc<DsqlConnectionDirector>, DsqlRunRepository, DsqlProjectionLog, MigrationRunner)` that decomposes the store into owned components
    - This resolves the ownership boundary: `HistoryNotifyingRepository::new` needs `Arc<R>` and `DsqlVisibilityStore::new` needs `Arc<DsqlConnectionDirector>` — the current facade only exposes borrowed references
    - Add doc comment explaining WHY `into_parts` exists (tokeirad needs to distribute owned handles to runtime, edge, and projection subsystems that outlive the `DsqlStore` value)
    - _Requirements: 8.2.2, 8.4.5_

  - [x] 7.3 Make `MigrationRunner::status` handle missing `schema_version` table
    - In `crates/tokeira-storage/src/dsql/migration.rs`, update `status()` to catch the "relation does not exist" error from `SELECT max(version) FROM schema_version`
    - When the table is absent, return `SchemaStatus { current_version: None, checked_at }` instead of propagating the error
    - Add doc comment explaining WHY this is handled gracefully (fresh databases have no schema_version table until the first migration runs)
    - _Requirements: 5.2.1, 5.2.2_

  - [x] 7.4 Implement DSQL startup branch in `tokeirad`
    - In `apps/tokeirad/src/main.rs` (or `lib.rs`), branch on `infrastructure.storage`:
      - `InMemory` → existing `InMemoryStore::default()` path
      - `Dsql` → construct `DsqlAuthConfig` from config, call `DsqlStore::connect(auth, DsqlPoolConfig::default()).await`, call `into_parts()` to get owned components
    - Wire `DsqlRunRepository`: wrap in `Arc`, pass to `HistoryNotifyingRepository::new(Arc<DsqlRunRepository>, waits)`
    - Wire `DsqlProjectionLog`: the type must be shareable across projection workers. Either derive `Clone` on `DsqlProjectionLog` (it holds only `Arc<DsqlConnectionDirector>`) or wrap in `Arc` with a blanket `ProjectionLog` impl for `Arc<T>`. Each worker gets a clone.
    - Wire `DsqlVisibilityStore`: construct with `DsqlVisibilityStore::new(Arc::clone(&director))` from the `into_parts()` director. `DsqlVisibilityStore` SHALL be made shareable (derive `Clone` — it holds only `Arc<DsqlConnectionDirector>` — or implement `VisibilityStore` + `ProjectionSink` for `Arc<DsqlVisibilityStore>`). The same instance (or clones) is shared between `VisibilityQueryService` and all projection workers.
    - Enable `dsql` feature on `tokeira-projection` dependency in `apps/tokeirad/Cargo.toml`
    - Exit non-zero on connection failure; do NOT fall back to InMemory
    - Add doc comments explaining the storage selection flow and WHY pool config uses defaults (storage-layer config boundary)
    - _Requirements: 8.2.1, 8.2.2, 8.2.3, 8.2.4, 8.2.5, 8.2.6, 8.4.1, 8.4.2, 8.4.3, 8.4.4, 8.4.5, 8.4.6_

  - [x] 7.5 Add storage backend startup log message
    - Log `INFO` message before gRPC bind: `"storage backend: in-memory"` or `"storage backend: dsql"` with endpoint field
    - Ensure no role ARNs or credentials appear in the log
    - _Requirements: 8.3.1, 8.3.2, 8.3.3_

  - [ ]* 7.6 Write property test for ConfigStorageKind round-trip (Property 8)
    - **Property 8: ConfigStorageKind TOML round-trip**
    - Generate `TokeiraConfig` with both storage variants
    - Assert serialize-then-deserialize equivalence
    - **Validates: Requirements 8.1.6**

  - [ ]* 7.7 Write property test for DSQL validation (Property 9)
    - **Property 9: DSQL storage validation rejects missing endpoint**
    - Generate `TokeiraConfig` with `Dsql` storage and empty/None endpoint
    - Assert `validate()` returns error referencing `"infrastructure.dsql.endpoint"`
    - **Validates: Requirements 8.1.4**

  - [ ]* 7.8 Write property test for startup log safety (Property 11)
    - **Property 11: Startup log does not leak sensitive fields**
    - Generate configs with DSQL role ARNs set
    - Assert the log message contains the endpoint but not any role ARN values
    - **Validates: Requirements 8.3.2**

- [ ] 8. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement schema setup CLI wiring
  - [x] 9.1 Add schema CLI dependencies to `apps/tkr/Cargo.toml`
    - Add `tokeira-storage` dependency with `dsql` feature enabled (for `MigrationRunner`, `DsqlAuthConfig`, `DsqlPoolConfig`)
    - Add `sqlx` with `runtime-tokio`, `tls-rustls`, `postgres` features
    - Add `aurora-dsql-sqlx-connector` for IAM-authenticated PgPool construction
    - _Requirements: 5.1.2_

  - [x] 9.2 Implement `tkr schema setup` command handler
    - Add `schema` subcommand group to `apps/tkr/src/cli.rs` with `setup`, `status`, and `validate` subcommands
    - Implement `setup` handler: load `tokeirad.toml`, read endpoint and region from `infrastructure.dsql` (region is always explicit after writeback), construct `PgPool` with IAM auth, construct `MigrationRunner`, call `apply(&pool).await`, report applied count
    - Require `--yes` or interactive confirmation before executing
    - Return error if endpoint not configured: "dsql endpoint is not configured in {path}; run `tkr infra apply --module dsql` first"
    - _Requirements: 5.1.1, 5.1.2, 5.1.3, 5.1.4_

  - [x] 9.3 Implement `tkr schema status` command handler
    - Connect to DSQL endpoint (same path as setup)
    - Call `runner.status(&pool).await`
    - Report schema version or "not initialized" message
    - _Requirements: 5.2.1, 5.2.2, 5.2.3_

  - [x] 9.4 Implement `tkr schema validate` command handler
    - Call `runner.validate()` (no pool required)
    - Report validation issues or "All migrations valid"
    - Exit 0 on success, exit 1 on issues
    - _Requirements: 5.3.1, 5.3.2, 5.3.3, 5.3.4_

  - [x] 9.5 Implement migration embedding via `build.rs` in `tokeira-storage`
    - Add `sha2` to `[build-dependencies]` in `crates/tokeira-storage/Cargo.toml` (build scripts cannot use normal crate dependencies)
    - Add `build.rs` that discovers `migrations/V{nnn}__{name}.sql` files
    - Validate version sequence (no gaps, no duplicates); fail build on violations
    - Compute SHA-256 checksums at build time using `sha2::Sha256`
    - Emit `&'static [EmbeddedMigration]` array into `$OUT_DIR/migrations_embedded.rs`
    - Emit `cargo:rerun-if-changed=migrations`
    - Include the generated file in `tokeira_storage::dsql::migration` via `include!`
    - Add `MigrationRunner::embedded()` constructor that loads the compile-time array (production path). Retain `MigrationRunner::new(MigrationConfig { migrations_dir })` for tests only.
    - Add doc comment on the embedding mechanism explaining WHY checksums are pre-computed (avoids runtime hashing and filesystem access)
    - _Requirements: 5.1.5_

- [x] 10. Implement prototypical config generation for compose+dsql
  - [x] 10.1 Add `--region` argument to `tkr deployment create`
    - Add `--region <region>` optional argument to `DeploymentAction::Create` in `apps/tkr/src/cli.rs`
    - The `PlatformConfig::prototypical_config(storage)` trait method signature is NOT changed — it remains platform-agnostic with only `StorageKind` as input
    - Instead, after generating the base config via `prototypical_config(storage)`, the CLI handler patches the TOML string to set `region = "<value>"` in the `[dsql]` section when `--region` is provided. Use `toml_edit` (already in the workspace) to parse, set the field, and re-serialize.
    - When `--region` is not provided, the generated `[dsql]` section keeps the default `region = "us-east-1"` from `ComposeDsqlConfig::default()`
    - _Requirements: 2.1.1, 2.2.2_

  - [x] 10.2 Update `PlatformConfig::prototypical_config()` for DSQL storage
    - When `storage == StorageKind::Dsql`, generate `deployment.toml` with `storage = "dsql"` and `[dsql]` section containing `mode = "managed"`, empty endpoint/arn fields, and `region = "us-east-1"` (the default — non-default region is patched by the CLI in task 10.1)
    - When `storage == StorageKind::InMemory`, omit `[dsql]` section
    - Update `prototypical_server_config()` to include `infrastructure.storage = "dsql"` and `[infrastructure.dsql]` section when DSQL
    - _Requirements: 2.2.2, 2.2.3, 6.1.1_

- [ ] 11. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Improve observability dashboards
  - [x] 12.1 Refactor existing dashboards with better panel layouts
    - Reorganize `platforms/compose/dashboards/broker-runtime-health.json` with proper row grouping and consistent panel sizing
    - Reorganize `platforms/compose/dashboards/grpc-edge-health.json` with proper row grouping
    - Reorganize `platforms/compose/dashboards/storage-projection-health.json` with proper row grouping
    - Add more insightful queries: sync-match ratio as percentage, error rate as percentage of total, queue depth by namespace/task_queue
    - Ensure all datasource UIDs use `mimir` (matching provisioned datasource)
    - Reference EKS project's `server.json` for panel layout patterns (row grouping, stat+timeseries pairs)
    - _Requirements: (additional task — dashboard polish)_

  - [x] 12.2 Create a Loki log exploration dashboard
    - Create `platforms/compose/dashboards/log-exploration.json`
    - Include panels: log volume by service (bar chart), error/warn rate over time, log explorer panel with label filters
    - Use datasource UID `loki` (matching provisioned datasource)
    - Reference EKS project's `operator-console.json` for log panel patterns
    - _Requirements: (additional task — dashboard polish)_

- [x] 13. Update documentation
  - [x] 13.1 Update README.md with compose+DSQL deployment lifecycle
    - Add a "Compose + DSQL" section documenting both the two-phase and one-shot workflows from Req 6.1
    - Update the architecture section to mention DSQL as a storage option alongside in-memory
    - Add prerequisites section mentioning AWS credentials for DSQL mode
    - Update the existing "Storage and schema" paragraph to reflect first-class DSQL support
    - _Requirements: 6.1.7, (additional task — README updates)_

  - [x] 13.2 Update AGENTS.md with DSQL-related changes
    - Update "Workspace Structure" section to reflect the DSQL module in compose platform
    - Update "Configuration" section to mention `deployment.toml` DSQL fields and `tokeirad.toml` writeback
    - Update "Observability Stack" section to mention the improved dashboards and Loki log exploration
    - Add note about `DsqlModule` in the "Adding a New IaC Module" working agreement
    - _Requirements: (additional task — AGENTS.md updates)_

- [x] 14. Add code documentation
  - [x] 14.1 Add doc comments to all new public types and functions
    - `DsqlModule` — explain it provisions or adopts a DSQL cluster for compose deployments
    - `ComposeDsqlConfig` — explain the managed/preexisting distinction and field semantics
    - `DsqlMode` — explain each variant's lifecycle implications
    - `ConfigStorageKind` — explain it is the server-config-level selector distinct from orchestrator's `StorageKind`
    - `compose_services()` DSQL-conditional logic — inline comments explaining credential mounting rationale (Chainguard base, nonroot user, provider chain)
    - tokeirad startup branching — doc comments explaining the storage selection flow and pool config boundary
    - Module-level doc comment on any new module files
    - _Requirements: (additional task — code documentation)_

- [ ] 15. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- The design uses Rust throughout — no language selection needed
- Dashboard improvements reference the EKS project at `/Users/iw/Projects/temporalio/temporal-dsql-deploy-eks/grafana/` for panel layout inspiration but use tokeira-specific metric names (`tokeira_*` prefix)
- Datasource UIDs must be `mimir` and `loki` (matching the provisioned datasources in the compose platform)
- Code documentation follows AGENTS.md rules: comments explain WHY, not WHAT

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["1.3", "2.1", "2.2"] },
    { "id": 2, "tasks": ["2.3", "2.4", "2.5", "2.6", "2.7"] },
    { "id": 3, "tasks": ["4.1", "4.2", "5.1"] },
    { "id": 4, "tasks": ["4.3", "4.4", "4.5"] },
    { "id": 5, "tasks": ["7.1", "7.2", "7.3"] },
    { "id": 6, "tasks": ["7.4", "7.5"] },
    { "id": 7, "tasks": ["7.6", "7.7", "7.8"] },
    { "id": 8, "tasks": ["9.1", "9.5"] },
    { "id": 9, "tasks": ["9.2", "9.3", "9.4"] },
    { "id": 10, "tasks": ["10.1", "10.2"] },
    { "id": 11, "tasks": ["12.1", "12.2"] },
    { "id": 12, "tasks": ["13.1", "13.2"] },
    { "id": 13, "tasks": ["14.1"] }
  ]
}
```
