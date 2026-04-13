# Implementation Plan: Proto Upstream Sync

## Overview

Replace the hand-written Temporal API proto subset with the full upstream proto tree vendored from `buf.build/temporalio/api`. This involves building a Rust CLI sync tool, updating the build pipeline to compile from a workspace-level proto tree with glob discovery, expanding `public.rs` to export the full `temporal.api.*` hierarchy, migrating the translation layer, and removing the duplicate crate-local proto tree.

## Tasks

- [x] 1. Create the proto-sync CLI tool and register it in the workspace
  - [x] 1.1 Add `tools/proto-sync` to workspace members in `Cargo.toml`
    - Add `"tools/proto-sync"` to the `members` list in the root `Cargo.toml`
    - _Requirements: 1.8_

  - [x] 1.2 Create `tools/proto-sync/Cargo.toml`
    - Package name `proto-sync`, edition 2024, dependency on `anyhow = "1"`
    - _Requirements: 1.8_

  - [x] 1.3 Implement `tools/proto-sync/src/main.rs`
    - Parse version argument from CLI args; print usage and exit non-zero if missing
    - Find workspace root by walking up from the binary's location or using `CARGO_MANIFEST_DIR` at build time
    - Clean the `proto/upstream/` directory before writing new files
    - Shell out to `buf export buf.build/temporalio/api:<version> --output <upstream_dir>` to fetch protos and transitive deps
    - Write the version string to `proto/UPSTREAM_VERSION` on success
    - Exit non-zero with descriptive stderr messages on any failure (network, invalid version, missing `buf`, IO errors)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 2.1, 2.2, 2.3_

- [x] 2. Checkpoint — verify proto-sync builds and runs
  - Ensure `cargo build -p proto-sync` succeeds. Ask the user to run `cargo run -p proto-sync -- <version>` with a real version to populate `proto/upstream/`. Ask the user if questions arise.

- [x] 3. Update the build pipeline to compile from the workspace-level proto tree
  - [x] 3.1 Add `walkdir` and `prost-types` dependencies in `crates/tokeira-proto/Cargo.toml`
    - Add `walkdir = "2"` under `[build-dependencies]`
    - Add `prost-types = "0.12"` under `[dependencies]` (required for `google.protobuf.Timestamp` and `google.protobuf.Duration` in upstream-generated code)
    - _Requirements: 4.1, 4.5_

  - [x] 3.2 Rewrite `crates/tokeira-proto/build.rs` for glob-based proto discovery
    - Implement workspace root discovery: walk up from `CARGO_MANIFEST_DIR` looking for root `Cargo.toml` with `[workspace]`
    - Use `walkdir` to glob all `*.proto` files under `proto/upstream/`
    - Compile all discovered upstream protos with `tonic-build`, using `proto/upstream/` as the include path
    - Compile internal protos under `proto/tokeira/` with include paths `proto/` and `proto/upstream/`
    - Generate file descriptor sets for both public and internal surfaces
    - Add `cargo:rerun-if-changed` directives for the `proto/upstream/` and `proto/tokeira/` directories
    - Remove all hardcoded proto file paths from the current build.rs
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

- [x] 4. Expand `public.rs` to export the full upstream module hierarchy
  - [x] 4.1 Update `crates/tokeira-proto/src/public.rs` with all `temporal.api.*` modules
    - Add `tonic::include_proto!` declarations for all upstream packages: activity, batch, command, common, enums, errordetails, failure, filter, history, namespace, nexus, operatorservice, query, schedule, sdk, taskqueue, update, version, workflow, workflowservice (and any others present after sync)
    - Preserve existing convenience re-exports (`common`, `enums`, `history`, `workflowservice`, `operatorservice`) and service name constants
    - Keep `FILE_DESCRIPTOR_SET`, `http_proxy_path`, and HTTP service constants unchanged
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [x] 4.2 Update `crates/tokeira-proto/src/internal.rs` include paths if needed
    - Ensure `pub use crate::public::temporal;` still works with the expanded public module
    - Verify internal proto `tonic::include_proto!` calls are unaffected
    - _Requirements: 4.3_

  - [x] 4.3 Update `crates/tokeira-proto/src/lib.rs` re-exports if needed
    - Ensure the top-level `pub use` statements remain valid with the expanded public module
    - _Requirements: 5.2_

- [x] 5. Checkpoint — verify the workspace builds with the new proto tree
  - Ensure `cargo build -p tokeira-proto` succeeds. Ensure all existing tests pass with `cargo test -p tokeira-proto`. Ask the user if questions arise.

- [x] 6. Migrate the translation layer to upstream-generated types
  - [x] 6.1 Add well-known type conversion helpers to `crates/tokeira-proto/src/conversions/common.rs`
    - Add `to_proto_timestamp(time::OffsetDateTime) -> prost_types::Timestamp` helper
    - Add `to_proto_duration(time::Duration) -> prost_types::Duration` helper
    - Add `to_opt_proto_timestamp(Option<time::OffsetDateTime>) -> Option<prost_types::Timestamp>` helper
    - Add `to_opt_proto_duration(Option<time::Duration>) -> Option<prost_types::Duration>` helper
    - Add `..Default::default()` to any proto struct literals that gain new fields from the upstream types (e.g. `WorkflowExecution`, `TaskQueue`, `Payload`, `Payloads`, `Memo`, `SearchAttributes`)
    - Verify all conversion functions compile against the upstream-generated types
    - _Requirements: 6.3, 6.4, 6.5_

  - [x] 6.2 Update `crates/tokeira-edge/src/translate/history_serializer.rs`
    - Replace `to_unix_nanos()` calls with `to_proto_timestamp()` for `event_time` (field type changes from `i64` to `Option<prost_types::Timestamp>`)
    - Replace `opt_duration_millis()` calls with `to_opt_proto_duration()` for all `*_timeout` fields (field types change from `i64` to `Option<prost_types::Duration>`)
    - Replace `fire_at_unix_nanos: i64` with `fire_at: Option<prost_types::Timestamp>` in `TimerStartedEventAttributes`
    - Add `..Default::default()` to all proto struct literals that gain new upstream fields
    - Ensure `history_event_to_proto` and `serialize_history` compile against upstream types
    - This is the largest migration (~1850 lines with many attribute struct constructions)
    - _Requirements: 6.1, 6.4, 6.5_

  - [x] 6.3 Update `crates/tokeira-edge/src/translate/from_internal.rs`
    - Migrate any proto type references to use upstream-generated types
    - Replace `i64` timestamp/duration fields with well-known type conversions where applicable
    - Add `..Default::default()` where upstream types introduce new fields
    - _Requirements: 6.2, 6.5_

  - [x] 6.4 Update `crates/tokeira-edge/src/translate/to_internal.rs`
    - Migrate any proto type references to use upstream-generated types
    - Replace `i64` timestamp/duration fields with well-known type conversions where applicable
    - Add `..Default::default()` where upstream types introduce new fields
    - _Requirements: 6.2, 6.5_

  - [x] 6.5 Update `crates/tokeira-edge/src/grpc/translate.rs`
    - Replace inline `to_unix_nanos()` with `to_proto_timestamp()` for `start_time`, `close_time`, `execution_time` fields in `WorkflowExecutionInfo` construction
    - Update `workflow_execution_info_from_description` and `workflow_execution_info_from_summary` for upstream field type changes
    - Add `..Default::default()` where upstream types introduce new fields
    - _Requirements: 6.2, 6.5_

  - [x] 6.6 Delete dead conversion files `crates/tokeira-proto/src/conversions/workflow.rs` and `operator.rs`
    - These files exist but are not exported from `conversions/mod.rs` (which only has `pub mod common;`)
    - The edge crate has its own inline copies of the same functions in `grpc/translate.rs`
    - Delete both files to avoid confusion about which conversions are authoritative
    - _Requirements: 6.3_

  - [ ]* 6.7 Write property test for history event serialization round-trip
    - **Property 1: History event serialization round-trip**
    - **Validates: Requirements 6.4, 6.6**
    - Implement `proptest::Strategy` for `HistoryEvent` and `HistoryEventKind` covering all variant kinds
    - Test that `history_event_to_proto` → `encode_to_vec` → `decode` produces an equivalent proto message
    - Use `proptest` with minimum 100 cases
    - Tag: `Feature: proto-upstream-sync, Property 1: History event serialization round-trip`

- [x] 7. Checkpoint — verify translation layer compiles and tests pass
  - Ensure `cargo build` and `cargo test` pass for the full workspace. Ask the user if questions arise.

- [x] 8. Remove the crate-local proto tree and finalize
  - [x] 8.1 Delete `crates/tokeira-proto/proto/` directory
    - Remove the entire `crates/tokeira-proto/proto/` directory (including `README.md`, `upstream/`, and `tokeira/` subtrees)
    - _Requirements: 7.1, 7.3_

  - [x] 8.2 Verify build.rs has no references to `crates/tokeira-proto/proto/`
    - Confirm the rewritten build.rs (from task 3.2) contains no paths referencing the old crate-local proto directory
    - _Requirements: 7.2_

  - [x]* 8.3 Run full workspace build and test suite
    - Ensure `cargo build` and `cargo test` pass after the crate-local proto tree is removed
    - _Requirements: 7.4_

- [x] 9. Final checkpoint — full workspace validation
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The sync CLI requires `buf` installed on the developer's machine; `build.rs` does not
- Task 2 is a user-interactive checkpoint — the developer must run the sync tool to populate `proto/upstream/` before tasks 3+ can proceed
- The `history_serializer.rs` migration (task 6.2) is the largest single change (~1850 lines of struct literal updates)
- Property test (task 6.7) uses `proptest`, which is already idiomatic in the workspace
- **P0 dependency**: `prost-types` must be added to `tokeira-proto` dependencies (task 3.1) before the build can succeed with upstream protos — the generated code references `prost_types::Timestamp` and `prost_types::Duration`
- **Well-known type conversions**: The migration is not just `..Default::default()` — timestamp fields change from `i64` to `Option<prost_types::Timestamp>` and duration fields from `i64` to `Option<prost_types::Duration>`, requiring explicit conversion helpers (task 6.1)
- **Dead code cleanup**: `conversions/workflow.rs` and `operator.rs` are not exported from `mod.rs` and are unused — task 6.6 deletes them
