# Implementation Plan: TKR CLI Redesign

## Overview

Replace the prototype `tkr` CLI with a lifecycle-staged CLI featuring named deployment management under XDG-compliant paths, `dev` workspace commands, and lifecycle-staged operator commands that delegate to the orchestrator framework. Implementation follows the phased delivery from the requirements: core structure → dev commands → deployment management → infra → deploy (local) → deploy (compose) → schema → operational commands.

## Tasks

> Completion note: checked items are implemented in the task-sheet module
> layout unless the task itself describes broader provider behavior still left
> unchecked below.

- [x] 1. Rename `deployments/` to `platforms/` and update all references
  - Rename the `deployments/` directory to `platforms/`
  - Update `Cargo.toml` workspace member paths from `deployments/` to `platforms/`
  - Update `apps/tkr/Cargo.toml` dependency path from `../../deployments/local` to `../../platforms/local`
  - Update all `use` statements, doc comments, and references across the workspace that mention `deployments/`
  - Create `.cargo/config.toml` with `lint` alias if it does not exist (e.g., `lint = "clippy --workspace --all-targets"`)
  - _Requirements: 9.3.1, 9.3.2, 9.3.3_

- [x] 2. Move PlatformKind and StorageKind to tokeira-orchestrator
  - Move `PlatformKind` and `StorageKind` enums from the CLI to `crates/tokeira-orchestrator/src/lib.rs` with `Serialize`, `Deserialize`, `Clone`, `Debug`, `PartialEq`, `Eq` derives
  - Move `PlatformConfig` trait to `tokeira-orchestrator` with `prototypical_config(storage: StorageKind) -> String` and `prototypical_server_config(storage: StorageKind) -> String` methods
  - Platform crates import these from `tokeira-orchestrator` (no dependency cycle)
  - The CLI re-exports them and adds `clap::ValueEnum` derive via a newtype or wrapper
  - _Requirements: 3.1.1_

- [x] 2b. Extract `TokeiraConfig` into shared library crate `crates/tokeira-server-config/`
  - Move the `TokeiraConfig` struct, all sub-structs, AND all inherent impls (`resolve`, `load`, `validate`, `to_toml`, `to_redacted_json`, `emergency_warnings`), `ConfigError`, `ValidationError`, default functions, and the `redact_sensitive_fields` helper into `crates/tokeira-server-config/src/lib.rs`
  - Move all config-related tests into the new crate
  - `apps/tokeirad/` depends on `tokeira-server-config` and imports the types — no re-implementation
  - Platform crates depend on `tokeira-server-config` for `prototypical_server_config` template validation in tests
  - `apps/tkr/` depends on `tokeira-server-config` for loading `tokeirad.toml`
  - Add crate as workspace member in root `Cargo.toml`
  - _Requirements: 3.2.2_

- [x] 3. Set up new module structure and core CLI skeleton
  - [x] 3.1 Replace `apps/tkr/src/main.rs` with new entry point
    - Strip the existing prototype code from `main.rs`
    - Implement the new `main()`: parse `Cli` via clap, dispatch to command handlers
    - Add `mod` declarations for all modules: `cli`, `deployment_dir`, `metadata`, `prototypical`, `output`, `commands`, `process`
    - _Requirements: 1.1.1, 1.1.5, 1.1.7_

  - [x] 3.2 Create `cli.rs` with `Cli` struct and `Command` enum
    - Define `Cli` with global `--deployment` and `--json` flags using clap derive
    - Define `Command` enum with all top-level subcommands: `Dev`, `Deployment`, `Infra`, `Deploy`, `Schema`, `Scale`, `Logs`, `PortForward`, `Config`, `Version`
    - Define sub-enums: `DevAction`, `DeploymentAction`, `InfraAction`, `DeployAction`, `SchemaAction`, `ScaleAction`, `ConfigAction`
    - Define `CliPlatformKind` and `CliStorageKind` as CLI-local `clap::ValueEnum` wrapper enums, with `From<CliPlatformKind> for PlatformKind` and `From<CliStorageKind> for StorageKind` conversions to the orchestrator types
    - _Requirements: 1.1.1, 1.1.2, 1.1.4, 1.1.5_

  - [x] 3.3 Create `output.rs` with `OutputFormatter`
    - Implement `OutputFormatter` with `json: bool` field
    - Implement `print<T: Serialize + Display>` — JSON serialization when `--json`, `Display` otherwise
    - Implement `print_table` for aligned text tables or JSON array of objects
    - Implement `print_error` for error formatting to stderr
    - _Requirements: 1.1.4_

  - [x] 3.4 Update `apps/tkr/Cargo.toml` with new dependencies
    - Add `directories`, `uuid`, `serde_json`, `chrono` (or use manual ISO 8601), `libc`
    - Ensure `clap` has `derive` feature
    - Ensure `tokio` has `signal` and `process` features
    - Add `proptest` and `tempfile` to `[dev-dependencies]`
    - _Requirements: 1.1.5, 1.2.1_

- [x] 4. Implement `DeploymentResolver` and `.latest` tracking
  - [x] 4.1 Create `deployment_dir.rs` with `DeploymentResolver`
    - Use `directories::ProjectDirs::from("", "tokeira", "tkr")` to resolve the state directory as `Deployments_Root`
    - Implement `resolve_name`: explicit `--deployment` flag > `.latest` file > error with guidance
    - Implement `deployment_dir` returning `{root}/{name}/`
    - Implement `read_latest` / `write_latest` / `clear_latest` for `.latest` file management
    - Implement `list_deployments` scanning subdirectories of `Deployments_Root`
    - Implement `create_deployment`: create directory, generate `deployment.toml` and `tokeirad.toml` from prototypical templates, write `metadata.json`, create `state/` subdirectory, update `.latest`
    - Implement `remove_deployment`: remove directory, clear `.latest` if it was the latest
    - Implement `normalize_name`: lowercase + replace spaces with hyphens
    - Create `Deployments_Root` on first use if it does not exist
    - _Requirements: 1.2.1, 1.2.2, 1.2.3, 1.2.4, 1.2.5, 1.3.1, 1.3.2, 1.3.3, 1.4.1, 1.4.2, 1.4.3, 1.4.4, 1.4.5, 1.5.1, 1.5.2, 1.5.3, 1.5.4_

  - [ ]* 4.2 Write property test: deployment create round-trip (Property 1)
    - **Property 1: Deployment create round-trip — directory exists and .latest updated**
    - Generate random valid names and valid platform × storage combinations
    - After `create_deployment`, assert directory exists with `deployment.toml`, `tokeirad.toml`, `metadata.json`, `state/`
    - Assert `resolve_name(None)` returns the normalized name
    - Use `tempfile::TempDir` as the root
    - **Validates: Requirements 1.3.1, 1.4.1, 1.4.2, 1.4.4, 1.5.1, 3.1.3**

  - [ ]* 4.3 Write property test: deployment name normalization (Property 2)
    - **Property 2: Deployment name normalization**
    - Generate random strings with mixed case and spaces
    - Assert result is entirely lowercase and contains no spaces
    - Assert determinism: `normalize_name(x) == normalize_name(x)`
    - **Validates: Requirements 1.5.3**

  - [ ]* 4.4 Write property test: duplicate deployment name rejection (Property 3)
    - **Property 3: Duplicate deployment name rejection**
    - Create a deployment, then attempt to create another with the same normalized name
    - Assert the second creation returns an error
    - **Validates: Requirements 1.5.4**

  - [ ]* 4.5 Write property test: deployment destroy removes directory and clears .latest (Property 4)
    - **Property 4: Deployment destroy removes directory and clears .latest**
    - Create a deployment, then destroy it
    - Assert directory no longer exists
    - Assert `.latest` is empty or absent if the destroyed deployment was the latest
    - **Validates: Requirements 3.5.3, 3.5.4**

  - [ ]* 4.6 Write property test: non-existent deployment error lists available deployments (Property 5)
    - **Property 5: Non-existent deployment error lists available deployments**
    - Create a set of deployments, then attempt to resolve a name not in the set
    - Assert the error message contains every existing deployment name
    - **Validates: Requirements 1.4.5, 3.4.2, 9.2.2**

- [x] 5. Implement `DeploymentMetadata` and prototypical configs
  - [x] 5.1 Create `metadata.rs` with `DeploymentMetadata`
    - Define `DeploymentMetadata` struct with `platform`, `storage`, `status`, `created_at` fields
    - Define `DeploymentStatus` enum: `Created`, `Running`, `Stopped` (no `Destroyed` — destroy removes the directory entirely)
    - Implement `read` / `write` functions for `metadata.json` via `serde_json`
    - _Requirements: 9.1.1, 9.1.2_

  - [ ]* 5.4 Write property test: metadata round-trip (Property 8)
    - **Property 8: Deployment metadata round-trip**
    - Generate arbitrary `DeploymentMetadata` values (random platform, storage, status, ISO 8601 timestamp)
    - Serialize to JSON and deserialize back, assert equality
    - **Validates: Requirements 9.1.1**

  - [x] 5.2 Create `prototypical.rs` — delegates to platform crates
    - The CLI does NOT generate config itself — it calls the platform crate's `prototypical_config(storage)` for `deployment.toml` and `prototypical_server_config(storage)` for `tokeirad.toml`
    - Implement dispatch: match on `CliPlatformKind` to call the correct platform's template functions
    - Validate that `ecs + in-memory` is rejected at `deployment create` time
    - Each platform crate's `prototypical_config` template MUST produce TOML that deserializes into that platform's config type
    - Each platform crate's `prototypical_server_config` template MUST produce TOML that deserializes into `TokeiraConfig`
    - _Requirements: 3.2.1, 3.2.2, 3.2.3, 3.2.4, 3.1.5_

  - [x] 5.3 Add per-service `replicas` field to platform config types
    - Extend `LocalConfig` (and compose service definitions) with a per-service `replicas: u32` field (default 1)
    - Ensure prototypical templates include `replicas` for each service
    - `scale up` without arguments reads replica counts from this field
    - _Requirements: 8.1.1_

  - [x]* 5.5 Write unit tests for prototypical config templates
    - Verify each `prototypical_config` template produces valid TOML that deserializes into the platform's config type (round-trip test)
    - Verify each `prototypical_server_config` template produces valid TOML that deserializes into `TokeiraConfig` from `tokeira-server-config`
    - Verify `ecs + in-memory` combination is rejected
    - _Requirements: 3.2.1, 3.2.2, 3.1.5_

- [ ] 6. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Implement `dev` commands
  - [x] 7.1 Create `commands/dev.rs` with dev command handlers
    - Implement `dev build`: spawn `cargo build --workspace`, inherit stdio, exit with child's code
    - Implement `dev test`: spawn `cargo test --workspace` or `cargo test -p <name>` when `--crate` is provided
    - Implement `dev lint`: spawn `cargo clippy --workspace --all-targets` (or `cargo lint` if alias exists), inherit stdio
    - Implement `dev fmt`: spawn `cargo +nightly fmt`, inherit stdio
    - Implement `dev check`: spawn `cargo check --workspace`, inherit stdio
    - _Requirements: 2.1.1, 2.1.2, 2.2.1, 2.2.2, 2.2.3, 2.3.1, 2.3.2, 2.4.1, 2.4.2, 2.5.1, 2.5.2_

  - [ ]* 7.2 Write property test: dev test command construction with --crate (Property 9)
    - **Property 9: Dev test command construction with --crate**
    - Generate random non-empty crate name strings
    - Assert the constructed command line contains `cargo test -p <name>` with the exact crate name
    - **Validates: Requirements 2.2.2**

  - [x]* 7.3 Write unit tests for dev command clap parsing
    - Verify `tkr dev build`, `tkr dev test`, `tkr dev test --crate foo`, `tkr dev lint`, `tkr dev fmt`, `tkr dev check` all parse correctly
    - _Requirements: 2.1.1, 2.2.1, 2.2.2, 2.3.1, 2.4.1, 2.5.1_

- [ ] 8. Implement `deployment` commands
  - [x] 8.1 Create `commands/deployment.rs` with deployment command handlers
    - Implement `deployment create`: validate platform × storage, call `DeploymentResolver::create_deployment`, print result
    - Implement `deployment list`: scan deployments, read metadata, print table with `*` for latest
    - Implement `deployment use <name>`: validate name exists, update `.latest`
    - Implement `deployment destroy`: require `--yes`, stop if running, run `infra destroy` if infrastructure exists, remove directory, clear `.latest` if needed
    - _Requirements: 3.1.1, 3.1.2, 3.1.3, 3.1.4, 3.1.5, 3.3.1, 3.3.2, 3.3.3, 3.3.4, 3.4.1, 3.4.2, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5_

  - [x]* 8.2 Write unit tests for deployment command clap parsing
    - Verify `tkr deployment create --platform local --storage in-memory`, `tkr deployment list`, `tkr deployment use foo`, `tkr deployment destroy --yes` all parse correctly
    - _Requirements: 3.1.1, 3.1.2_

- [x] 9. Implement `infra` commands
  - [x] 9.1 Create `commands/infra.rs` with infra command handlers
    - Implement `infra plan`: resolve deployment, load config, delegate to `InfraEngine::plan`, format output
    - Implement `infra apply`: require `--yes`, delegate to `InfraEngine::apply`, collect writeback key-value pairs, write DSQL endpoint and other infra outputs into `tokeirad.toml` at the deployment directory path using `toml_edit` for lossless round-trip (preserving comments and unrelated fields)
    - Implement `infra destroy`: require `--yes`, delegate to `InfraEngine::destroy`
    - Implement `infra status`: read infrastructure state, print summary
    - Handle `local + in-memory` case: report no infrastructure required
    - Support optional `--module <name>` flag for scoping
    - _Requirements: 4.1.1, 4.1.2, 4.1.3, 4.2.1, 4.2.2, 4.2.3, 4.2.4, 4.2.5, 4.3.1, 4.3.2, 4.3.3, 4.4.1_

  - [x]* 9.2 Write unit tests for infra command clap parsing
    - Verify `tkr infra plan`, `tkr infra apply --yes`, `tkr infra destroy --yes`, `tkr infra status`, and `--module` flag parse correctly
    - _Requirements: 4.1.1, 4.1.2_

- [ ] 10. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Implement `deploy` commands for local platform
  - [x] 11.1 Create `process.rs` with local platform process management
    - Implement `spawn_tokeirad`: use `tokio::process::Command` to spawn tokeirad as a blocking foreground child with `--config <path>` pointing at the deployment's `tokeirad.toml`, inherit stdio
    - Implement SIGINT forwarding: install `tokio::signal::ctrl_c` handler, forward to child via `libc::kill`, wait with 5-second timeout, send SIGTERM if needed
    - Implement `write_pid_file` / `check_pid_file` / `remove_pid_file` for PID file management in the deployment directory
    - Update metadata status to `running` on start, `stopped` on exit
    - _Requirements: 5.1.1, 5.1.2, 5.1.3, 5.1.4, 5.1.5, 5.3.1_

  - [x] 11.2 Create `commands/deploy.rs` with deploy command handlers
    - Implement `deploy plan` for local: print tokeirad configuration that would be used
    - Implement `deploy apply` for local: call `spawn_tokeirad`, block until exit
    - Implement `deploy status` for local: check PID file, report running/stopped
    - Branch on platform from metadata: local vs compose (compose implemented in next task group)
    - _Requirements: 5.1.1, 5.1.2, 5.2.1, 5.3.1_

  - [x]* 11.3 Write unit tests for deploy command clap parsing
    - Verify `tkr deploy plan`, `tkr deploy apply --yes`, `tkr deploy status` parse correctly
    - _Requirements: 5.1.1, 5.2.1, 5.3.1_

- [ ] 12. Implement `deploy` commands for compose platform
  - [ ] 12.1 Add compose platform support to `commands/deploy.rs`
    - Implement `deploy plan` for compose: delegate to `DeployEngine::plan`, print service summary
    - Implement `deploy apply` for compose: require `--yes`, delegate to `DeployEngine::apply` via the platform's `Deployment` impl, return immediately (non-blocking)
    - Implement `deploy status` for compose: query container state via bollard, print service table
    - Update metadata status to `running` after stack is up
    - _Requirements: 6.1.1, 6.1.2, 6.1.3, 6.1.4, 6.2.1, 6.3.1_

- [ ] 13. Implement `schema` commands
  - [ ] 13.1 Create `commands/schema.rs` with schema command handlers
    - Implement `schema setup`: validate storage is `dsql`, run DSQL schema migration tooling against cluster endpoint from `tokeirad.toml`
    - Implement `schema status`: validate storage is `dsql`, query migration tracking table, print applied migrations
    - Return error with clear message when storage is not `dsql`
    - _Requirements: 7.1.1, 7.1.2, 7.1.3, 7.2.1, 7.2.2_

  - [ ]* 13.2 Write property test: schema commands reject non-DSQL storage (Property 6)
    - **Property 6: Schema commands reject non-DSQL storage**
    - Generate deployments with storage other than `dsql`
    - Assert `schema setup` and `schema status` return errors indicating schema management is only for DSQL
    - **Validates: Requirements 7.1.3, 7.2.2**

- [ ] 14. Implement operational commands
  - [ ] 14.1 Create `commands/scale.rs` with scale command handlers
    - Add `desired_replicas(config) -> Vec<ServiceReplicas>` method to the `Ops` trait in `tokeira-orchestrator` and implement it in the local platform
    - Ensure `desired_replicas` returns services in platform-defined startup order, with dependency services before dependents
    - Implement `scale up` without arguments: call `Ops::desired_replicas(config)`, then `scale_up` for each service in startup order
    - Implement `scale up <service> <n>`: scale named service to `n` replicas
    - Implement `scale down`: scale all services to 0, update status to `stopped`
    - Implement `scale status`: query container state, print replica counts and readiness
    - Return error listing valid service names when service not found
    - _Requirements: 8.1.1, 8.1.2, 8.1.3, 8.2.1, 8.2.2, 8.3.1_

  - [ ] 14.2 Create `commands/logs.rs` with logs command handler
    - Implement `logs <service>`: stream logs via bollard with `--follow` and `--tail` flags
    - Return error listing valid service names when service not found
    - _Requirements: 8.4.1, 8.4.2, 8.4.3_

  - [x] 14.3 Create `commands/port_forward.rs` with port-forward command handler
    - Change `Ops::port_forward` signature in `tokeira-orchestrator` from `port_forward(service, port, config) -> PortForwardTarget` to `port_mappings(service, config) -> Vec<PortMapping>` (returns all mappings for the service, no port argument)
    - Implement `port-forward <service>`: call `Ops::port_mappings`, print all host→container port mappings
    - Return error listing valid service names when service not found
    - _Requirements: 8.5.1, 8.5.2_

  - [x] 14.4 Create `commands/config.rs` with config command handler
    - Implement `config show`: read and print the deployment's `deployment.toml` and `tokeirad.toml` to stdout
    - _Requirements: 8.6.1_

  - [x] 14.5 Create `commands/version.rs` with version command handler
    - Implement `version`: print crate version from `Cargo.toml` and exit
    - _Requirements: 1.1.6_

  - [ ]* 14.6 Write property test: invalid service name error lists valid alternatives (Property 7)
    - **Property 7: Invalid service name error lists valid alternatives**
    - Generate strings that are not valid service names
    - Assert commands (`logs`, `port-forward`, `scale up`) return errors listing every valid service name
    - **Validates: Requirements 8.1.3, 8.4.3, 8.5.2**

  - [x]* 14.7 Write unit tests for operational command clap parsing
    - Verify `tkr scale up`, `tkr scale down`, `tkr scale status`, `tkr logs <service>`, `tkr port-forward <service>`, `tkr config show`, `tkr version` all parse correctly
    - _Requirements: 8.1.1, 8.2.1, 8.3.1, 8.4.1, 8.5.1, 8.6.1, 1.1.6_

- [ ] 15. Wire lifecycle command flow and error handling
  - [x] 15.1 Implement the lifecycle command dispatch in `main.rs`
    - For commands requiring deployment context: resolve name via `DeploymentResolver`, load `metadata.json`, load `deployment.toml` and `tokeirad.toml`, construct `Deployment` impl, delegate to handler
    - For `dev` and `version` commands: dispatch directly without deployment context
    - Format all output through `OutputFormatter`
    - _Requirements: 1.1.2, 1.1.3, 1.1.4, 9.1.3, 9.1.4_

  - [ ] 15.2 Implement error handling patterns
    - Deployment not found: list available deployments, suggest `deployment create`
    - `.latest` missing/stale: instruct operator to use `--deployment` or `deployment create`
    - Invalid platform × storage: reject with explanation
    - Missing external tool: detect via `which`, print installation guidance
    - Confirmation required: print what would happen, require `--yes`
    - Config parse errors: print file path and parse error
    - _Requirements: 9.2.1, 9.2.2, 9.2.3, 9.2.4_

- [ ] 16. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests use `proptest` with minimum 100 iterations (`ProptestConfig::with_cases(100)`)
- The CLI communicates through the platform's `Deployment` impl, never directly to `InfraEngine`/`DeployEngine`
- Comments should explain WHY, not WHAT
- Do NOT put `use` statements in function scope
