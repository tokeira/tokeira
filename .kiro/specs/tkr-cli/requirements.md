# Requirements Document: TKR CLI Redesign

## Introduction

The `tkr` CLI is the single entry point for all tokeira operations — both developer workspace actions and operator deployment lifecycle management. The current `tkr` CLI in `apps/tkr/` is a basic prototype. This spec replaces it with an ergonomic CLI organized around lifecycle-staged command groups: separate `infra`, `deploy`, `schema`, and `scale` commands that respect the IaC framework's lifecycle stages.

The redesigned CLI introduces:

- A `dev` command group for developer workspace actions (build, test, lint, fmt, check) that delegate to cargo.
- A `deployment` command group for creating and managing named deployment instances.
- Lifecycle-staged operator commands (`infra`, `deploy`, `schema`, `scale`) that operate on the selected deployment.
- Operational commands (`logs`, `port-forward`, `config`) for runtime access.
- Named deployment tracking under XDG Base Directory paths, with a `.latest` file for implicit deployment resolution.
- A global `--deployment <name>` flag for selecting the target deployment (defaults to `.latest`).
- Platform × storage matrix: `local`, `compose`, and `ecs` platforms combined with `in-memory` and `dsql` storage backends.
- Prototypical config generation from built-in platform × storage templates.

### CLI Structure

```
tkr [--deployment <name>] [--json]
├── dev                              # Developer workspace (no deployment context)
│   ├── build
│   ├── test [--crate <name>]
│   ├── lint
│   ├── fmt
│   └── check
│
├── deployment                       # Deployment management
│   ├── create --platform <p> --storage <s> [--name <n>]
│   ├── list
│   ├── use <name>
│   └── destroy [--yes]
│
├── infra                            # Infrastructure lifecycle
│   ├── plan [--module <name>]
│   ├── apply [--module <name>] [--yes]
│   ├── destroy [--module <name>] [--yes]
│   └── status
│
├── deploy                           # Service deployment
│   ├── plan
│   ├── apply [--yes]
│   └── status
│
├── schema                           # Schema lifecycle (DSQL only)
│   ├── setup
│   └── status
│
├── scale                            # Service scaling
│   ├── up [<service>] [<n>]
│   ├── down
│   └── status
│
├── logs <service>                   # Stream logs (--follow, --tail)
├── port-forward <service>           # Forward ports
│
├── config                           # Config management
│   └── show
│
└── version
```

### Lifecycle Stages by Platform × Storage

Not all stages are required for every combination. The platform determines which stages are relevant:

| Platform × Storage | `infra apply` | `deploy apply` | `schema setup` | `scale up` |
|---|---|---|---|---|
| local + in-memory | skip | spawns tokeirad (blocks) | n/a | n/a |
| local + dsql | provisions DSQL cluster | spawns tokeirad (blocks) | required | n/a |
| compose + in-memory | provisions compose network | deploys containers | n/a | optional |
| compose + dsql | provisions DSQL + compose | deploys containers | required | optional |
| ecs + dsql | provisions full AWS infra | deploys ECS services | required | required |

### Phased Delivery

- Phase 1: Core CLI structure (clap), deployment directory management, XDG paths, `.latest` tracking, global `--deployment` flag, extract `TokeiraConfig` into shared `crates/tokeira-server-config/` library crate
- Phase 2: `dev` commands (build, test, lint, fmt, check)
- Phase 3: `deployment create/list/use/destroy` with prototypical config generation
- Phase 4: `infra plan/apply/destroy/status` — infrastructure lifecycle
- Phase 5: `deploy plan/apply/status` for local platform (blocking child process)
- Phase 6: `deploy plan/apply/status` for compose platform (bollard)
- Phase 7: `schema setup/status` for DSQL storage
- Phase 8: `scale up/down/status`, `logs`, `port-forward`, `config show`

### What This Spec Does NOT Cover

- ECS platform implementation (future)
- TUI mode (future — the spec covers CLI only)
- Global tkr preferences (`$XDG_CONFIG_HOME/tokeira/tkr.toml`) — deferred
- DSQL cluster provisioning implementation (covered by dsql-schema-connection spec)
- The orchestrator framework internals (covered by orchestrator-framework spec)

## Glossary

- **TKR_CLI**: The `tkr` binary in `apps/tkr/`, the single entry point for all tokeira developer and operator commands.
- **Deployment**: A named, tracked instance of a tokeira environment with its own config, state directory, and platform × storage combination. Stored under the XDG state directory.
- **Deployment_Directory**: The filesystem directory for a single deployment, containing `deployment.toml` (platform config), `tokeirad.toml` (server runtime config), `state/`, and platform-specific artifacts (e.g., `docker-compose.yml` for compose).
- **Deployments_Root**: The top-level directory under `$XDG_STATE_HOME/tokeira/tkr/` (default: `~/.local/state/tokeira/tkr/` on Linux, `~/Library/Application Support/tokeira/tkr/` on macOS) containing all deployment directories and the `.latest` file.
- **Latest_File**: A file at `{Deployments_Root}/.latest` containing the name of the most recently used deployment. Used for implicit deployment resolution when `--deployment` is omitted.
- **Platform**: The execution environment for a deployment. One of `local` (spawns tokeirad as a blocking child process), `compose` (Docker Compose stack via bollard), or `ecs` (AWS ECS — future).
- **Storage**: The persistence backend for a deployment. One of `in-memory` (ephemeral, built into tokeirad) or `dsql` (Aurora DSQL — requires cluster provisioning and schema setup).
- **Prototypical_Config**: A built-in config template for each platform × storage combination. `deployment create` generates the deployment's `deployment.toml` and `tokeirad.toml` from this template.
- **XDG_Base_Directory**: The XDG Base Directory Specification for platform-correct user data paths. Implemented via the `directories` crate (`ProjectDirs`).
- **Deployment_Status**: The current state of a deployment: `created` (exists but never started), `running` (currently active), or `stopped` (was running, now halted, state preserved). Destroy removes the directory entirely — there is no persistent `destroyed` state.
- **Collect_Writeback**: The mechanism by which infrastructure outputs (e.g., a DSQL cluster endpoint provisioned during `infra apply`) are written back into the deployment's `tokeirad.toml`.
- **Lifecycle_Stage**: One of the ordered stages in the deployment lifecycle: infrastructure provisioning (`infra`), service deployment (`deploy`), schema migration (`schema`), and service scaling (`scale`). Not all stages are required for every platform × storage combination.

## Requirements

---

## Phase 1: Core CLI Structure and Deployment Directory Management

### Requirement 1.1: Top-Level Command Structure

**User Story:** As a tokeira developer, I want a CLI with lifecycle-staged command groups and a global deployment selector, so that I can operate on any deployment with clear separation between infrastructure, deployment, schema, and operational concerns.

#### Acceptance Criteria

1. THE TKR_CLI SHALL define top-level subcommands: `dev`, `deployment`, `infra`, `deploy`, `schema`, `scale`, `logs`, `port-forward`, `config`, and `version`.
2. THE TKR_CLI SHALL accept a global `--deployment <name>` flag that selects the target deployment for all commands except `dev`, `deployment`, and `version`.
3. WHEN `--deployment` is omitted, THE TKR_CLI SHALL resolve the deployment from the Latest_File.
4. THE TKR_CLI SHALL accept a global `--json` flag that switches output to structured JSON for scripting.
5. THE TKR_CLI SHALL use `clap` with derive macros for argument parsing.
6. WHEN `tkr version` is invoked, THE TKR_CLI SHALL print the crate version from `Cargo.toml` and exit.
7. WHEN `tkr` is invoked with no subcommand, THE TKR_CLI SHALL print the help message listing available subcommands.

### Requirement 1.2: XDG-Compliant Deployment Root

**User Story:** As a tokeira operator, I want deployment data stored under XDG-compliant paths, so that deployment state follows platform conventions on Linux and macOS.

#### Acceptance Criteria

1. THE TKR_CLI SHALL resolve the Deployments_Root using the `directories` crate's `ProjectDirs` with qualifier `""`, organization `"tokeira"`, and application `"tkr"`, selecting the state directory.
2. WHEN `$XDG_STATE_HOME` is set on Linux, THE TKR_CLI SHALL store deployments under `$XDG_STATE_HOME/tokeira/tkr/`.
3. WHEN `$XDG_STATE_HOME` is not set on Linux, THE TKR_CLI SHALL store deployments under `~/.local/state/tokeira/tkr/`.
4. WHEN running on macOS, THE TKR_CLI SHALL store deployments under `~/Library/Application Support/tokeira/tkr/`.
5. THE TKR_CLI SHALL create the Deployments_Root directory on first use if it does not exist.

### Requirement 1.3: Deployment Directory Layout

**User Story:** As a tokeira operator, I want each deployment to have a self-contained directory with config and state, so that deployments are isolated and portable.

#### Acceptance Criteria

1. WHEN a deployment is created, THE TKR_CLI SHALL create a Deployment_Directory at `{Deployments_Root}/{name}/`.
2. THE Deployment_Directory SHALL contain a `deployment.toml` file (platform/orchestrator config) generated from the Prototypical_Config for the chosen platform × storage combination.
3. THE Deployment_Directory SHALL contain a `tokeirad.toml` file (server runtime config matching the `TokeiraConfig` schema from the configuration-foundation spec) generated from the platform's server config template.
4. THE Deployment_Directory SHALL contain a `state/` subdirectory for infrastructure and deployment state persistence (local backend).
5. WHEN the platform is `compose`, THE Deployment_Directory SHALL also contain a `docker-compose.yml` file generated during infrastructure lifecycle operations.

### Requirement 1.4: Latest Deployment Tracking

**User Story:** As a tokeira operator, I want a `.latest` file that tracks the most recently used deployment, so that I can omit the `--deployment` flag and operate on the current deployment by default.

#### Acceptance Criteria

1. THE TKR_CLI SHALL maintain a Latest_File at `{Deployments_Root}/.latest` containing the name of the latest deployment.
2. WHEN `deployment create` succeeds, THE TKR_CLI SHALL update the Latest_File to the newly created deployment's name.
3. WHEN `deployment use <name>` is invoked, THE TKR_CLI SHALL update the Latest_File to the specified deployment's name.
4. WHEN a command requires a deployment and `--deployment` is omitted, THE TKR_CLI SHALL resolve the deployment name from the Latest_File.
5. IF the Latest_File does not exist or references a deployment that no longer exists, THEN THE TKR_CLI SHALL return an error instructing the operator to specify `--deployment` or run `deployment create`.

### Requirement 1.5: Deployment Naming

**User Story:** As a tokeira operator, I want to name my deployments with human-readable names or let the CLI generate a UUID, so that I can manage multiple environments by name.

#### Acceptance Criteria

1. WHEN `deployment create` is invoked with `--name <name>`, THE TKR_CLI SHALL use the provided name as the deployment identifier and directory name.
2. WHEN `deployment create` is invoked without `--name`, THE TKR_CLI SHALL generate a UUID and use it as the deployment identifier and directory name.
3. THE TKR_CLI SHALL normalize deployment names by converting to lowercase and replacing spaces with hyphens.
4. IF a deployment with the given name already exists, THEN THE TKR_CLI SHALL return an error indicating the name is taken.

---

## Phase 2: Developer Workspace Commands

### Requirement 2.1: Dev Build Command

**User Story:** As a tokeira developer, I want `tkr dev build` to compile the entire workspace, so that I have a single command for building all crates.

#### Acceptance Criteria

1. WHEN `tkr dev build` is invoked, THE TKR_CLI SHALL execute `cargo build --workspace` as a child process, inheriting stdin, stdout, and stderr.
2. THE TKR_CLI SHALL exit with the same exit code as the cargo process.

### Requirement 2.2: Dev Test Command

**User Story:** As a tokeira developer, I want `tkr dev test` to run tests with optional crate scoping, so that I can run all tests or focus on a single crate.

#### Acceptance Criteria

1. WHEN `tkr dev test` is invoked without `--crate`, THE TKR_CLI SHALL execute `cargo test --workspace` as a child process.
2. WHEN `tkr dev test --crate <name>` is invoked, THE TKR_CLI SHALL execute `cargo test -p <name>` as a child process.
3. THE TKR_CLI SHALL exit with the same exit code as the cargo process.

### Requirement 2.3: Dev Lint Command

**User Story:** As a tokeira developer, I want `tkr dev lint` to run the project linter, so that I can check code quality with a single command.

#### Acceptance Criteria

1. WHEN `tkr dev lint` is invoked, THE TKR_CLI SHALL execute `cargo clippy --workspace --all-targets` as a child process, inheriting stdin, stdout, and stderr. If the `.cargo/config.toml` `lint` alias exists, `cargo lint` may be used instead.
2. THE TKR_CLI SHALL exit with the same exit code as the lint process.

### Requirement 2.4: Dev Fmt Command

**User Story:** As a tokeira developer, I want `tkr dev fmt` to format the workspace using the nightly formatter, so that formatting is consistent.

#### Acceptance Criteria

1. WHEN `tkr dev fmt` is invoked, THE TKR_CLI SHALL execute `cargo +nightly fmt` as a child process, inheriting stdin, stdout, and stderr.
2. THE TKR_CLI SHALL exit with the same exit code as the cargo process.

### Requirement 2.5: Dev Check Command

**User Story:** As a tokeira developer, I want `tkr dev check` to type-check the workspace without producing binaries, so that I get fast feedback on compilation errors.

#### Acceptance Criteria

1. WHEN `tkr dev check` is invoked, THE TKR_CLI SHALL execute `cargo check --workspace` as a child process, inheriting stdin, stdout, and stderr.
2. THE TKR_CLI SHALL exit with the same exit code as the cargo process.

---

## Phase 3: Deployment Management

### Requirement 3.1: Deployment Create Command

**User Story:** As a tokeira operator, I want `deployment create` to initialize a new tracked deployment with a platform and storage selection, so that I can set up environments without manual config authoring.

#### Acceptance Criteria

1. THE `deployment create` command SHALL require `--platform <local|compose|ecs>` and `--storage <in-memory|dsql>` arguments.
2. THE `deployment create` command SHALL accept an optional `--name <name>` argument.
3. WHEN `deployment create` is invoked, THE TKR_CLI SHALL create the Deployment_Directory, generate `deployment.toml` and `tokeirad.toml` from the Prototypical_Config, create the `state/` subdirectory, and update the Latest_File.
4. THE TKR_CLI SHALL NOT start the deployment during `deployment create` — creation and lifecycle stages are separate operations.
5. IF the platform is `ecs` and the storage is `in-memory`, THEN THE TKR_CLI SHALL return an error because ECS requires DSQL storage.

### Requirement 3.2: Prototypical Config Generation

**User Story:** As a tokeira operator, I want `deployment create` to generate deployment and server config files from a built-in template, so that I have a working starting point without needing to know the config schema.

#### Acceptance Criteria

1. EACH platform crate SHALL expose a `fn prototypical_config(storage: StorageKind) -> String` function that returns valid TOML deserializable into that platform's config type (e.g., `LocalConfig` for the local platform). This generates `deployment.toml`.
2. EACH platform crate SHALL also expose a `fn prototypical_server_config(storage: StorageKind) -> String` function that returns valid TOML deserializable into `TokeiraConfig`. This generates `tokeirad.toml`.
3. THE generated `tokeirad.toml` SHALL be a valid `TokeiraConfig` that can be used without further editing for the `in-memory` storage variant.
4. WHEN the storage is `dsql`, THE generated `tokeirad.toml` SHALL contain placeholder values for DSQL-specific settings that are populated by Collect_Writeback after `infra apply`.

### Requirement 3.3: Deployment List

**User Story:** As a tokeira operator, I want `deployment list` to show all deployments with their platform, storage, status, and creation date, so that I can see what environments exist at a glance.

#### Acceptance Criteria

1. WHEN `deployment list` is invoked, THE TKR_CLI SHALL scan the Deployments_Root and print a table with columns: NAME, PLATFORM, STORAGE, STATUS, CREATED.
2. THE TKR_CLI SHALL mark the latest deployment with an asterisk (`*`) next to its name.
3. THE TKR_CLI SHALL read platform, storage, and creation date from each deployment's metadata.
4. THE TKR_CLI SHALL determine status by checking whether the deployment's services are currently running.

### Requirement 3.4: Deployment Use

**User Story:** As a tokeira operator, I want `deployment use <name>` to set a deployment as the latest, so that subsequent commands operate on it by default.

#### Acceptance Criteria

1. WHEN `deployment use <name>` is invoked with a valid deployment name, THE TKR_CLI SHALL update the Latest_File to the specified name.
2. IF the specified deployment does not exist, THEN THE TKR_CLI SHALL return an error listing available deployments.

### Requirement 3.5: Deployment Destroy

**User Story:** As a tokeira operator, I want `deployment destroy` to tear down all resources and remove the deployment directory, so that I can clean up environments I no longer need.

#### Acceptance Criteria

1. WHEN `deployment destroy` is invoked, THE TKR_CLI SHALL stop the deployment if it is running.
2. WHEN the deployment has provisioned infrastructure, THE TKR_CLI SHALL run `infra destroy` before removing the deployment directory.
3. WHEN `deployment destroy` completes, THE TKR_CLI SHALL remove the Deployment_Directory from the filesystem.
4. IF the destroyed deployment was the latest, THEN THE TKR_CLI SHALL clear the Latest_File.
5. THE `deployment destroy` command SHALL require explicit confirmation via a `--yes` flag or interactive prompt before proceeding.

---

## Phase 4: Infrastructure Lifecycle

### Requirement 4.1: Infra Plan

**User Story:** As a tokeira operator, I want `infra plan` to show what infrastructure changes would be made without applying them, so that I can review before committing.

#### Acceptance Criteria

1. WHEN `infra plan` is invoked, THE TKR_CLI SHALL compute the diff between desired and actual infrastructure state and print a human-readable plan showing resources to create, update, or delete.
2. THE `infra plan` command SHALL accept an optional `--module <name>` flag to scope the plan to a specific infrastructure module.
3. WHEN the deployment uses `local` platform with `in-memory` storage, THE TKR_CLI SHALL report that no infrastructure is required.

### Requirement 4.2: Infra Apply

**User Story:** As a tokeira operator, I want `infra apply` to provision or update infrastructure for my deployment, so that the platform resources are ready for service deployment.

#### Acceptance Criteria

1. WHEN `infra apply` is invoked, THE TKR_CLI SHALL use the orchestrator framework's InfraEngine to provision infrastructure modules in dependency order.
2. THE `infra apply` command SHALL accept an optional `--module <name>` flag to scope the apply to a specific infrastructure module.
3. WHEN infrastructure provisioning produces outputs (e.g., DSQL endpoint), THE TKR_CLI SHALL write them back to the deployment's `tokeirad.toml` via Collect_Writeback.
4. THE `infra apply` command SHALL require explicit confirmation via a `--yes` flag or interactive prompt before proceeding.
5. WHEN the deployment uses `local` platform with `in-memory` storage, THE TKR_CLI SHALL skip infrastructure provisioning and report that no infrastructure is required.

### Requirement 4.3: Infra Destroy

**User Story:** As a tokeira operator, I want `infra destroy` to tear down provisioned infrastructure, so that I can clean up cloud resources.

#### Acceptance Criteria

1. WHEN `infra destroy` is invoked, THE TKR_CLI SHALL use the orchestrator framework's InfraEngine to destroy infrastructure modules in reverse dependency order.
2. THE `infra destroy` command SHALL accept an optional `--module <name>` flag to scope the destroy to a specific infrastructure module.
3. THE `infra destroy` command SHALL require explicit confirmation via a `--yes` flag or interactive prompt before proceeding.

### Requirement 4.4: Infra Status

**User Story:** As a tokeira operator, I want `infra status` to show the current infrastructure state, so that I can verify what resources are provisioned.

#### Acceptance Criteria

1. WHEN `infra status` is invoked, THE TKR_CLI SHALL read the infrastructure state from the deployment's state store and print a summary of provisioned resources.

---

## Phase 5: Service Deployment for Local Platform

### Requirement 5.1: Deploy Apply for Local Platform

**User Story:** As a tokeira developer, I want `deploy apply` on the local platform to spawn tokeirad as a blocking foreground process, so that I can see logs in my terminal and stop it with Ctrl+C.

#### Acceptance Criteria

1. WHEN `deploy apply` is invoked for a deployment with `platform = local`, THE TKR_CLI SHALL spawn the tokeirad binary as a blocking child process in the foreground.
2. THE TKR_CLI SHALL pass configuration to the tokeirad process via `--config <path>` pointing at the deployment's `tokeirad.toml` (delivered by the configuration-foundation spec).
3. THE TKR_CLI SHALL inherit stdin, stdout, and stderr so that the operator sees tokeirad logs directly in the terminal.
4. WHEN the operator sends SIGINT (Ctrl+C), THE TKR_CLI SHALL forward the signal to the tokeirad child process and wait for it to exit gracefully.
5. THE TKR_CLI SHALL update the deployment status to `running` when tokeirad starts and to `stopped` when it exits.

### Requirement 5.2: Deploy Plan for Local Platform

**User Story:** As a tokeira developer, I want `deploy plan` on the local platform to show what will happen when I apply, so that I can verify the configuration before starting.

#### Acceptance Criteria

1. WHEN `deploy plan` is invoked for a deployment with `platform = local`, THE TKR_CLI SHALL print the tokeirad configuration that would be used, including the storage backend and network addresses.

### Requirement 5.3: Deploy Status for Local Platform

**User Story:** As a tokeira developer, I want `deploy status` on the local platform to show whether tokeirad is running, so that I can check the current state.

#### Acceptance Criteria

1. WHEN `deploy status` is invoked for a deployment with `platform = local`, THE TKR_CLI SHALL check whether the tokeirad process is running (via PID file) and report the status.

---

## Phase 6: Service Deployment for Compose Platform

### Requirement 6.1: Deploy Apply for Compose Platform

**User Story:** As a tokeira operator, I want `deploy apply` on the compose platform to deploy the tokeirad + observability stack via bollard, so that I get a production-like local environment.

#### Acceptance Criteria

1. WHEN `deploy apply` is invoked for a deployment with `platform = compose`, THE TKR_CLI SHALL use the orchestrator framework's DeployEngine to deploy services via the Compose_Provider (bollard).
2. THE TKR_CLI SHALL return to the command prompt after the stack is running — `deploy apply` for compose is non-blocking.
3. THE `deploy apply` command SHALL require explicit confirmation via a `--yes` flag or interactive prompt before proceeding.
4. THE TKR_CLI SHALL update the deployment status to `running` after the stack is up.

### Requirement 6.2: Deploy Plan for Compose Platform

**User Story:** As a tokeira operator, I want `deploy plan` on the compose platform to show what services would be deployed, so that I can review before applying.

#### Acceptance Criteria

1. WHEN `deploy plan` is invoked for a deployment with `platform = compose`, THE TKR_CLI SHALL compute the service deployment plan and print a summary of services to create, update, or leave unchanged.

### Requirement 6.3: Deploy Status for Compose Platform

**User Story:** As a tokeira operator, I want `deploy status` on the compose platform to show the running services and their container status, so that I can monitor the stack.

#### Acceptance Criteria

1. WHEN `deploy status` is invoked for a deployment with `platform = compose`, THE TKR_CLI SHALL query container state via bollard and print a table of services with their status.

---

## Phase 7: Schema Management for DSQL Storage

### Requirement 7.1: Schema Setup

**User Story:** As a tokeira operator, I want `schema setup` to run DSQL schema migrations, so that the database is ready for use after infrastructure provisioning.

#### Acceptance Criteria

1. WHEN `schema setup` is invoked for a deployment with `storage = dsql`, THE TKR_CLI SHALL run the DSQL schema migration tooling against the cluster endpoint from the deployment's `tokeirad.toml`.
2. THE TKR_CLI SHALL NOT run schema setup automatically during any other lifecycle stage — schema management is an explicit operator action.
3. IF the deployment does not use `dsql` storage, THEN THE TKR_CLI SHALL return an error indicating that schema management is only available for DSQL deployments.

### Requirement 7.2: Schema Status

**User Story:** As a tokeira operator, I want `schema status` to show the current migration state, so that I can verify which migrations have been applied.

#### Acceptance Criteria

1. WHEN `schema status` is invoked for a deployment with `storage = dsql`, THE TKR_CLI SHALL query the migration tracking table and print the list of applied migrations with their versions and timestamps.
2. IF the deployment does not use `dsql` storage, THEN THE TKR_CLI SHALL return an error indicating that schema management is only available for DSQL deployments.

---

## Phase 8: Operational Commands

### Requirement 8.1: Scale Up

**User Story:** As a tokeira operator, I want `scale up` to start or increase service replicas, so that I can bring services online after deployment.

#### Acceptance Criteria

1. WHEN `scale up` is invoked without arguments for a compose deployment, THE TKR_CLI SHALL call `Ops::desired_replicas(config)` to get per-service target counts, then scale each service to its configured replica count in the order returned by `desired_replicas`.
2. THE `Ops::desired_replicas(config)` result SHALL be ordered in platform-defined startup order, with dependency services appearing before dependents.
3. WHEN `scale up <service> <n>` is invoked, THE TKR_CLI SHALL scale the named service to `n` replicas.
4. IF the named service does not exist, THEN THE TKR_CLI SHALL return an error listing valid service names.

### Requirement 8.2: Scale Down

**User Story:** As a tokeira operator, I want `scale down` to stop all services while preserving state, so that I can pause the environment.

#### Acceptance Criteria

1. WHEN `scale down` is invoked for a compose deployment, THE TKR_CLI SHALL scale all services to 0 replicas via bollard.
2. THE TKR_CLI SHALL update the deployment status to `stopped`.

### Requirement 8.3: Scale Status

**User Story:** As a tokeira operator, I want `scale status` to show current replica counts and readiness, so that I can verify service health.

#### Acceptance Criteria

1. WHEN `scale status` is invoked for a compose deployment, THE TKR_CLI SHALL query container state via bollard and print a table of services with their current replica count and readiness.

### Requirement 8.4: Logs

**User Story:** As a tokeira operator, I want `logs <service>` to stream logs from a running service, so that I can observe service behavior in real time.

#### Acceptance Criteria

1. WHEN `logs <service>` is invoked for a compose deployment, THE TKR_CLI SHALL stream logs from the named service's container via bollard with follow mode.
2. THE TKR_CLI SHALL accept `--follow` and `--tail <n>` flags.
3. IF the named service does not exist, THEN THE TKR_CLI SHALL return an error listing valid service names.

### Requirement 8.5: Port-Forward

**User Story:** As a tokeira operator, I want `port-forward <service>` to show port mappings for a running service, so that I can access services from my host machine.

#### Acceptance Criteria

1. WHEN `port-forward <service>` is invoked for a compose deployment, THE TKR_CLI SHALL read all port mappings from the running container's configuration via bollard and print each mapping (host port → container port).
2. IF the named service does not exist, THEN THE TKR_CLI SHALL return an error listing valid service names.

### Requirement 8.6: Config Show

**User Story:** As a tokeira operator, I want `config show` to print the resolved configuration for the current deployment, so that I can inspect the effective settings.

#### Acceptance Criteria

1. WHEN `config show` is invoked, THE TKR_CLI SHALL read and print both the deployment's `deployment.toml` and `tokeirad.toml` contents to stdout.

---

## Cross-Cutting Requirements

### Requirement 9.1: Deployment Status Tracking

**User Story:** As a tokeira operator, I want deployment status tracked persistently, so that the CLI can report accurate status across invocations.

#### Acceptance Criteria

1. THE TKR_CLI SHALL persist deployment metadata (platform, storage, status, creation timestamp) in a metadata file within each Deployment_Directory.
2. THE TKR_CLI SHALL update the status field when deployments transition between `created`, `running`, and `stopped` states.
3. FOR the `local` platform, THE TKR_CLI SHALL verify status by checking whether the tokeirad process is still running (via PID file), rather than relying solely on the persisted status.
4. FOR the `compose` platform, THE TKR_CLI SHALL verify status by querying container state via bollard, rather than relying solely on the persisted status.

### Requirement 9.2: Error Handling and User Feedback

**User Story:** As a tokeira operator, I want clear error messages with actionable guidance, so that I can resolve problems without reading source code.

#### Acceptance Criteria

1. WHEN a command fails, THE TKR_CLI SHALL print a descriptive error message to stderr including what went wrong and a suggested remediation.
2. WHEN a deployment is not found, THE TKR_CLI SHALL list available deployments in the error message.
3. WHEN a required external tool is missing (e.g., Docker for compose platform, cargo for dev commands), THE TKR_CLI SHALL detect the absence and print an error with installation guidance.
4. THE TKR_CLI SHALL use `anyhow` for error handling in the CLI binary and `thiserror` for error types in any library code extracted from the CLI.

### Requirement 9.3: Repo Directory Rename

**User Story:** As a tokeira contributor, I want the `deployments/` directory in the repo renamed to `platforms/`, so that the directory name reflects its contents (platform implementations) rather than conflicting with the runtime deployment concept.

#### Acceptance Criteria

1. THE repo directory `deployments/` SHALL be renamed to `platforms/`.
2. ALL workspace member paths in `Cargo.toml` referencing `deployments/` SHALL be updated to `platforms/`.
3. ALL import paths and documentation referencing `deployments/` SHALL be updated to `platforms/`.
