# Requirements Document

## Introduction

Tokeira currently maintains a hand-written subset of Temporal-compatible proto definitions inside `crates/tokeira-proto/proto/upstream/`. This covers only the RPCs and message types needed so far, but diverges from the canonical Temporal API surface and requires manual maintenance as new endpoints are added.

This feature replaces that hand-written subset with vendored upstream Temporal API protos fetched from `buf.build/temporalio/api`. The sync is performed by a Rust CLI tool that downloads a specified version, vendors the `.proto` files into the workspace-level `proto/upstream/temporal/api/` tree, and writes a `proto/UPSTREAM_VERSION` file for tracking. The vendored files are committed to the repo so that `buf` is not required at build time — only when updating the upstream version.

The build pipeline (`build.rs` + `tonic-build`) is then updated to compile the full upstream proto tree (including transitive `google/protobuf` dependencies) from the workspace-level `proto/` directory. The `public.rs` module is updated to export the richer upstream module structure, and all downstream translation code in `tokeira-edge` and `tokeira-proto/src/conversions/` is migrated to use the upstream-generated types.

## Glossary

- **Sync_CLI**: The Rust CLI binary that fetches upstream Temporal API proto files from `buf.build` and vendors them into the workspace proto tree.
- **Upstream_Version_File**: The `proto/UPSTREAM_VERSION` file at the workspace root containing the synced upstream version string (e.g. `v1.62.0`).
- **Upstream_Proto_Tree**: The directory `proto/upstream/temporal/api/` at the workspace root containing vendored `.proto` files from the Temporal API.
- **Crate_Proto_Tree**: The directory `crates/tokeira-proto/proto/` containing the current crate-local proto files (to be removed after migration).
- **Internal_Proto_Tree**: The directory `proto/tokeira/internal/` at the workspace root containing Tokeira-owned internal proto definitions.
- **Build_Script**: The `crates/tokeira-proto/build.rs` file that invokes `tonic-build` to compile proto files into Rust code.
- **Public_Module**: The `crates/tokeira-proto/src/public.rs` file that re-exports generated Temporal-compatible proto modules.
- **History_Serializer**: The module at `crates/tokeira-edge/src/translate/history_serializer.rs` that converts kernel events to proto history events.
- **Translation_Layer**: The modules under `crates/tokeira-edge/src/translate/` and `crates/tokeira-proto/src/conversions/` that convert between proto wire types and internal domain types.

## Requirements

### Requirement 1: Rust CLI Proto Sync Tool

**User Story:** As a developer, I want a Rust CLI tool that fetches upstream Temporal API protos from buf.build at a specified version, so that I can update vendored protos without manual file management or shell scripts.

#### Acceptance Criteria

1. WHEN invoked with a version argument (e.g. `v1.62.0`), THE Sync_CLI SHALL download the Temporal API proto files for that version from `buf.build/temporalio/api`.
2. THE Sync_CLI SHALL write the downloaded `.proto` files into the Upstream_Proto_Tree directory, preserving the upstream directory structure (e.g. `proto/upstream/temporal/api/common/v1/message.proto`).
3. WHEN the Upstream_Proto_Tree already contains files from a previous sync, THE Sync_CLI SHALL remove stale files before writing the new version.
4. THE Sync_CLI SHALL write the version string to the Upstream_Version_File after a successful sync.
5. WHEN the Upstream_Proto_Tree contains transitive dependencies (e.g. `google/protobuf/*.proto`, `google/api/*.proto`), THE Sync_CLI SHALL vendor those into `proto/upstream/` alongside the Temporal API files.
6. IF the download fails (network error, invalid version, missing module), THEN THE Sync_CLI SHALL exit with a non-zero status code and print a descriptive error message to stderr.
7. IF the version argument is omitted, THEN THE Sync_CLI SHALL print usage instructions and exit with a non-zero status code.
8. THE Sync_CLI SHALL be a workspace binary target (e.g. under `dev/proto-sync/` or `tools/proto-sync/`) buildable with `cargo build`.

### Requirement 2: Upstream Version Tracking

**User Story:** As a developer, I want a version file committed alongside the vendored protos, so that I can see which upstream version is checked in and diff against it.

#### Acceptance Criteria

1. THE Upstream_Version_File SHALL contain exactly one line: the version string written by the Sync_CLI (e.g. `v1.62.0`).
2. THE Upstream_Version_File SHALL be located at `proto/UPSTREAM_VERSION` relative to the workspace root.
3. WHEN the Sync_CLI completes successfully, THE Upstream_Version_File SHALL reflect the version that was just synced.
4. THE Upstream_Version_File SHALL be committed to version control alongside the vendored proto files.

### Requirement 3: Workspace-Level Proto Tree Consolidation

**User Story:** As a developer, I want a single proto tree at the workspace root that contains both upstream and internal protos, so that build.rs and buf tooling have one canonical source of truth.

#### Acceptance Criteria

1. THE Upstream_Proto_Tree SHALL reside at `proto/upstream/temporal/api/` relative to the workspace root.
2. THE Internal_Proto_Tree SHALL reside at `proto/tokeira/internal/` relative to the workspace root.
3. WHEN the migration is complete, THE Crate_Proto_Tree (`crates/tokeira-proto/proto/`) SHALL be removed from the repository.
4. THE Build_Script SHALL reference only the workspace-level `proto/` tree for all proto compilation.
5. THE `buf.yaml` at the workspace root SHALL reference the workspace-level `proto/upstream` and `proto/tokeira` modules.

### Requirement 4: Build Pipeline Update

**User Story:** As a developer, I want `build.rs` to compile the full upstream Temporal API proto tree using tonic-build, so that all upstream message types and service definitions are available as generated Rust code without requiring `buf` at build time.

#### Acceptance Criteria

1. THE Build_Script SHALL compile all `.proto` files under the Upstream_Proto_Tree using `tonic-build`.
2. THE Build_Script SHALL include `proto/upstream/` as a proto include path so that transitive imports (e.g. `google/protobuf/timestamp.proto`, `google/protobuf/duration.proto`) resolve correctly.
3. THE Build_Script SHALL continue to compile the Internal_Proto_Tree with `proto/` and `proto/upstream/` as include paths.
4. THE Build_Script SHALL generate file descriptor sets for both the public and internal proto surfaces.
5. WHEN a `.proto` file is added or removed from the Upstream_Proto_Tree, THE Build_Script SHALL detect the change and trigger recompilation via `cargo:rerun-if-changed` directives.
6. THE Build_Script SHALL compile successfully without `buf` or any external protobuf tooling installed on the developer machine.

### Requirement 5: Public Module Re-Export Update

**User Story:** As a crate consumer, I want `tokeira-proto::public` to export the full upstream Temporal API module hierarchy, so that all upstream types are accessible without manual module declarations.

#### Acceptance Criteria

1. THE Public_Module SHALL export all upstream `temporal.api.*` packages generated from the Upstream_Proto_Tree (e.g. `temporal::api::common::v1`, `temporal::api::failure::v1`, `temporal::api::taskqueue::v1`, `temporal::api::workflow::v1`).
2. THE Public_Module SHALL preserve the existing convenience re-exports (`common`, `enums`, `history`, `workflowservice`, `operatorservice`) for backward compatibility.
3. THE Public_Module SHALL export the `FILE_DESCRIPTOR_SET` constant for the public API surface.
4. WHEN new upstream packages are added by a proto sync, THE Public_Module SHALL expose them under the `temporal::api` module hierarchy.

### Requirement 6: Translation Layer Migration

**User Story:** As a developer, I want all type conversion and serialization code to use the upstream-generated proto types instead of the hand-written subset, so that the codebase has a single source of truth for Temporal wire types.

#### Acceptance Criteria

1. THE History_Serializer SHALL use upstream-generated `temporal.api.history.v1` types for history event serialization.
2. THE Translation_Layer modules in `tokeira-edge/src/translate/` SHALL use upstream-generated types for all request/response translation.
3. THE Translation_Layer modules in `tokeira-proto/src/conversions/` SHALL use upstream-generated types for payload, memo, search attribute, and status conversions.
4. WHEN an upstream-generated type replaces a hand-written type, THE replacement SHALL preserve the existing wire-format compatibility (same field names, same protobuf field numbers).
5. IF an upstream type introduces new fields not present in the hand-written subset, THEN THE Translation_Layer SHALL set those fields to their protobuf default values until explicit support is added.
6. FOR ALL valid Kernel_Event values, serializing to upstream proto bytes and then deserializing back SHALL produce an equivalent proto message (round-trip property preserved after migration).

### Requirement 7: Crate-Local Proto Tree Removal

**User Story:** As a developer, I want the duplicate crate-local proto tree removed after migration, so that there is no ambiguity about which proto files are authoritative.

#### Acceptance Criteria

1. WHEN the Build_Script successfully compiles from the workspace-level proto tree, THE Crate_Proto_Tree directory (`crates/tokeira-proto/proto/`) SHALL be deleted from the repository.
2. WHEN the Crate_Proto_Tree is removed, THE Build_Script SHALL contain no references to `crates/tokeira-proto/proto/`.
3. WHEN the Crate_Proto_Tree is removed, THE `crates/tokeira-proto/proto/README.md` SHALL be deleted.
4. THE workspace SHALL compile and all existing tests SHALL pass after the Crate_Proto_Tree is removed.
