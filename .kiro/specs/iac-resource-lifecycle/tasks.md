# Implementation Plan: IaC Resource Lifecycle

## Overview

Extend `tokeira-iac` with resource mode persistence, destroy-mode context propagation, incremental state saves, module-scoped delete suppression, CLI progress reporting with `indicatif`, and describe-before-delete safety. All changes are in Rust, targeting the `tokeira-iac` library crate, the `tokeira-aws` resource crate, the platform crates (`platforms/local`, `platforms/compose`, `tokeira-compose`), and the `tkr` CLI binary.

## Tasks

- [ ] 1. Add `ResourceMode` enum and extend `ResourceState`
  - [ ] 1.1 Define `ResourceMode` enum in `tokeira-iac`
    - Add `ResourceMode` enum (`Managed`, `Preexisting`, `Shared`) to `crates/tokeira-iac/src/lib.rs`
    - Derive `Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default`
    - Use `#[serde(rename_all = "snake_case")]` and `#[default] Managed`
    - Export `ResourceMode` from the crate root
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [ ] 1.2 Add `mode` field to `ResourceState`
    - Add `pub mode: ResourceMode` field to `ResourceState` in `crates/tokeira-iac/src/lib.rs`
    - Annotate with `#[serde(default)]` for backward compatibility with existing state files
    - _Requirements: 1.1, 1.5_

  - [ ] 1.3 Add `mode()` as a required method on the `Resource` trait
    - Add `fn mode(&self) -> ResourceMode;` to the `Resource` trait in `crates/tokeira-iac/src/lib.rs`
    - This is a required method — no default implementation
    - _Requirements: 8.4_

  - [ ]* 1.4 Write property test for `ResourceMode` serialization round-trip (Property 1)
    - **Property 1: ResourceMode Serialization Round-Trip**
    - **Validates: Requirements 1.6**
    - Use `proptest` with `prop_oneof![Just(Managed), Just(Preexisting), Just(Shared)]`
    - Verify `serde_json::from_str(&serde_json::to_string(&mode)?) == Ok(mode)` for all generated modes
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/lib.rs` `#[cfg(test)]` module

  - [ ]* 1.5 Write property test for `ResourceMode` backward compatibility default (Property 2)
    - **Property 2: ResourceMode Backward Compatibility Default**
    - **Validates: Requirements 1.5**
    - Use `proptest` to generate random `ResourceState` JSON objects without a `mode` field (random `physical_id`, `resource_type`, `properties`, `module`, timestamps)
    - Verify deserialized `ResourceState` has `mode == ResourceMode::Managed`
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/lib.rs` `#[cfg(test)]` module

- [ ] 2. Update all existing `Resource` implementations to add `mode()`
  - [ ] 2.1 Update `tokeira-aws` resource implementations
    - Add `fn mode(&self) -> ResourceMode { ResourceMode::Managed }` to all 12 resources in `crates/tokeira-aws/src/resources/`: `DsqlCluster`, `DsqlConnectionEndpoint`, `DynamoDbTable`, `EcrRepository`, `EksClusterResource`, `IamRole`, `PodIdentityAssociation`, `S3Bucket`, `SecretsManagerSecret`, `SecurityGroup`, `VpcEndpoint`, `VpcResource`
    - _Requirements: 8.4_

  - [ ] 2.2 Update `platforms/local` resource implementation
    - Add `fn mode(&self) -> iac::ResourceMode { iac::ResourceMode::Managed }` to `LocalStateResource` in `platforms/local/src/lib.rs`
    - _Requirements: 8.4_

  - [ ] 2.3 Update `platforms/compose` resource implementations
    - Add `fn mode(&self) -> iac::ResourceMode { iac::ResourceMode::Managed }` to `LocalStateResource` and `OwnedComposeResource` in `platforms/compose/src/modules.rs`
    - _Requirements: 8.4_

  - [ ] 2.4 Update `tokeira-compose` resource implementation
    - Add `fn mode(&self) -> iac::ResourceMode { iac::ResourceMode::Managed }` to `ComposeService` in `crates/tokeira-compose/src/lib.rs`
    - _Requirements: 8.4_

  - [ ] 2.5 Update test `Resource` implementations
    - Add `fn mode(&self) -> ResourceMode { ResourceMode::Managed }` to `FakeResource` in `crates/tokeira-iac/src/diff.rs` tests
    - Add `fn mode(&self) -> ResourceMode { ResourceMode::Managed }` to `FakeResource` in `crates/tokeira-iac/src/engine.rs` tests (make mode configurable via a field for later property tests)
    - Add `fn mode(&self) -> iac::ResourceMode { iac::ResourceMode::Managed }` to `TestResource` in `crates/tokeira-orchestrator/src/lib.rs` tests
    - _Requirements: 8.4_

- [ ] 3. Checkpoint — Ensure workspace compiles
  - Run `cargo clippy --workspace --all-targets` and verify the workspace compiles cleanly with the new `mode()` method on all `Resource` implementations.

- [ ] 4. Add `DestroyMode` marker and context propagation
  - [ ] 4.1 Define `DestroyMode` marker struct in `tokeira-iac`
    - Add `#[derive(Debug, Clone, Copy)] pub struct DestroyMode;` to `crates/tokeira-iac/src/lib.rs`
    - Export from crate root
    - _Requirements: 2.1_

  - [ ] 4.2 Register `DestroyMode` in engine destroy methods
    - In `Engine::destroy` and `Engine::destroy_known` and `Engine::destroy_for_modules` in `crates/tokeira-iac/src/engine.rs`, call `ctx.set_extension(DestroyMode)` before calling `collect_resources_from`
    - _Requirements: 2.1, 2.2_

  - [ ] 4.3 Write unit test for `DestroyMode` propagation through `ModuleContext`
    - Verify that when `DestroyMode` is set on `ProvisionContext`, `ModuleContext::extension::<DestroyMode>()` returns `Some`
    - Verify that when `DestroyMode` is not set, `ModuleContext::extension::<DestroyMode>()` returns `None`
    - Test location: `crates/tokeira-iac/src/module.rs` `#[cfg(test)]` module
    - _Requirements: 2.2, 2.5_

- [ ] 5. Implement mode-aware engine logic
  - [ ] 5.1 Persist `mode` in `ResourceState` during create and update
    - In `apply_changes` in `crates/tokeira-iac/src/engine.rs`, after `resource.create(ctx)` returns a `ResourceState`, set `rs.mode = resource.mode()` before inserting into `ctx.state`
    - Similarly after `resource.update(current, ctx)` returns, set `rs.mode = resource.mode()`
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [ ] 5.2 Skip delete for non-Managed resources in `apply_changes`
    - In the delete pass of `apply_changes`, before calling `resource.delete()`, check `resource.mode()` — skip the delete call if mode is `Preexisting` or `Shared`, but still remove from state
    - _Requirements: 2.4, 8.5_

  - [ ] 5.3 Skip delete for non-Managed resources in `destroy_changes`
    - In `destroy_changes`, before calling `resource.delete()`, check `resource.mode()` — if not `Managed`, remove from state without calling `delete()` and invoke the `StateSaver`
    - _Requirements: 2.4, 7.2, 8.5_

  - [ ]* 5.4 Write property test for mode persistence after mutation (Property 3)
    - **Property 3: Engine Persists Correct Mode After Mutation**
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4**
    - Use `proptest` to generate resources with random `ResourceMode` values
    - After simulated create/update, verify `ctx.state[rid].mode == resource.mode()`
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

  - [ ]* 5.5 Write property test for destroy excludes non-Managed resources (Property 4)
    - **Property 4: Destroy Excludes Non-Managed Resources**
    - **Validates: Requirements 2.4, 8.5**
    - Use `proptest` to generate resource sets with mixed modes (`Managed`, `Preexisting`, `Shared`)
    - Verify `delete()` is never called on resources with mode `Preexisting` or `Shared`
    - Verify the resulting change set never contains a `Delete` for non-Managed resources
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

- [ ] 6. Implement mode-aware diff logic
  - [ ] 6.1 Update `compute_changes` to consider `ResourceMode` during diff
    - In `compute_changes` in `crates/tokeira-iac/src/engine.rs`, when a resource exists in state, check the resource's `mode()`:
      - `Managed`: full property comparison via `resource.diff()` (existing behavior)
      - `Preexisting`: compare only engine-controlled fields (tags, associations); ignore provider-managed fields
      - `Shared`: report `NoChange` unless engine-controlled properties have diverged
    - _Requirements: 8.1, 8.2, 8.3_

  - [ ] 6.2 Suppress `Delete` changes for `Preexisting` resources in normal plan/apply
    - In `compute_changes`, when generating Delete changes for resources in state but not in desired, check the persisted `ResourceState.mode` — if `Preexisting` or `Shared`, emit `NoChange` instead of `Delete`
    - _Requirements: 8.5_

  - [ ]* 6.3 Write unit tests for mode-aware diff behavior
    - Test that `Managed` resources produce full diff (Create/Update/Delete)
    - Test that `Preexisting` resources never produce `Delete` during plan/apply
    - Test that `Shared` resources report `NoChange` unless engine-controlled properties diverge
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module
    - _Requirements: 8.1, 8.2, 8.3, 8.5_

- [ ] 7. Checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-iac` and `cargo clippy --workspace --all-targets` and verify all tests pass.

- [ ] 8. Formalize `StateSaver` incremental save contract
  - [ ] 8.1 Verify `StateSaver` is called after every create, update, and delete
    - Audit `apply_changes` and `destroy_changes` in `crates/tokeira-iac/src/engine.rs` to confirm the `StateSaver` callback is invoked after each individual mutation (create, update, delete)
    - Verify the saver is called after pruning stale resources during refresh (when `has_managed_missing` is true)
    - _Requirements: 3.1, 3.2, 3.3, 3.5_

  - [ ] 8.2 Verify `StateSaver` error aborts the engine
    - Audit that if `save(&ctx.state).await?` returns an error, the `?` operator propagates it immediately, aborting the apply/destroy loop
    - _Requirements: 3.4_

  - [ ]* 8.3 Write property test for `StateSaver` invocation count (Property 5)
    - **Property 5: StateSaver Invocation Count Equals Mutation Count**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.6**
    - Use `proptest` to generate random sequences of N resources (mix of creates, updates, deletes)
    - Use an `AtomicUsize` counter in the `StateSaver` callback
    - Verify `counter == N` after apply completes
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

  - [ ]* 8.4 Write property test for `StateSaver` error aborts engine (Property 6)
    - **Property 6: StateSaver Error Aborts Engine**
    - **Validates: Requirements 3.4**
    - Use `proptest` to generate random K ∈ [1, N] where the saver fails at invocation K
    - Verify the engine completes at most K mutating operations and returns an error
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

- [ ] 9. Formalize module-scoped delete suppression
  - [ ] 9.1 Verify `filter_changes_by_modules` behavior
    - Audit the existing `filter_changes_by_modules` function in `crates/tokeira-iac/src/engine.rs`
    - Confirm it suppresses Delete changes for resources whose persisted module is not in the active set
    - Confirm it preserves all Create, Update, and NoChange entries regardless of module scope
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

  - [ ]* 9.2 Write property test for module-scoped delete filtering (Property 7)
    - **Property 7: Module-Scoped Delete Filtering**
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
    - Use `proptest` to generate random sets of `Change` entries (mix of Create, Update, Delete, NoChange) with random module assignments, and random active module sets
    - Verify: filtered Delete changes are a subset of resources owned by active modules
    - Verify: all Create, Update, NoChange entries are preserved unchanged
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

- [ ] 10. Checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-iac` and `cargo clippy --workspace --all-targets` and verify all tests pass.

- [ ] 11. Implement CLI progress reporting with `indicatif`
  - [ ] 11.1 Add `console` and `indicatif` dependencies to `apps/tkr`
    - Add `console` and `indicatif` to `apps/tkr/Cargo.toml` dependencies
    - These are binary-crate-only dependencies — do not add to library crates
    - _Requirements: 6.1_

  - [ ] 11.2 Create `ActionTuiHandle` and `OutputFormat` in `apps/tkr/src/tui.rs`
    - Create new file `apps/tkr/src/tui.rs`
    - Define `OutputFormat` enum (`Human`, `Json`)
    - Define `ActionTuiHandle` struct wrapping `indicatif::MultiProgress` with counters for completed, failed, skipped
    - Define `ProgressEvent` enum with `serde::Serialize` for JSON output: `OperationStart`, `OperationComplete`, `WaitProgress`, `Note`, `Summary`
    - Implement `ActionTuiHandle::new(format: OutputFormat) -> Self`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.6_

  - [ ] 11.3 Implement `ActionTuiHandle::install` to wire progress reporters
    - Implement `install(&self, ctx: &mut ProvisionContext)` that sets `apply_progress`, `wait_progress`, and `note_progress` closures on the context
    - For `Human` format: create `ProgressBar` spinners with resource type and ID, replace with completion indicator showing elapsed time
    - For `Json` format: emit `ProgressEvent` JSON lines to stdout
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

  - [ ] 11.4 Implement `ActionTuiHandle::print_summary`
    - For `Human` format: print summary line with completed/failed/skipped counts and elapsed time using `console::style`
    - For `Json` format: emit `ProgressEvent::Summary` as JSON
    - _Requirements: 6.7_

  - [ ] 11.5 Wire `ActionTuiHandle` into `infra apply` and `infra destroy` commands
    - In `apps/tkr/src/commands/infra.rs`, create `ActionTuiHandle` based on `--json` flag
    - Call `tui.install(&mut ctx)` before calling the engine
    - Call `tui.print_summary()` after the engine returns
    - Add `mod tui;` to `apps/tkr/src/main.rs` or appropriate module root
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.7_

  - [ ]* 11.6 Write unit tests for `ActionTuiHandle` JSON output
    - Verify `OutputFormat::Json` produces valid JSON for all `ProgressEvent` variants
    - Verify summary counts match actual operation outcomes
    - Test location: `apps/tkr/src/tui.rs` `#[cfg(test)]` module
    - _Requirements: 6.6, 6.7_

- [ ] 12. Checkpoint — Ensure CLI compiles and tests pass
  - Run `cargo clippy --workspace --all-targets` and `cargo test -p tkr` and verify the CLI compiles cleanly with the new `tui` module.

- [ ] 13. Implement describe-before-delete with mode awareness
  - [ ] 13.1 Verify existing describe-before-delete in `destroy_changes`
    - Audit `destroy_changes` in `crates/tokeira-iac/src/engine.rs` to confirm:
      - `describe()` is called before `delete()` for each resource
      - If `describe()` returns `None`, the resource is pruned from state without calling `delete()`
      - If `describe()` returns `Some(live_state)`, `live_state` is passed to `delete()` instead of persisted state
      - If `describe()` returns an error, the error is propagated and destroy aborts for that resource
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [ ]* 13.2 Write property test for describe-before-delete count (Property 9)
    - **Property 9: Describe-Before-Delete Count**
    - **Validates: Requirements 7.1, 7.2, 7.5**
    - Use `proptest` to generate N resources in state where K are absent (describe returns `None`)
    - Verify `delete()` is called exactly N-K times
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

  - [ ]* 13.3 Write property test for describe-before-delete uses live state (Property 10)
    - **Property 10: Describe-Before-Delete Uses Live State**
    - **Validates: Requirements 7.3**
    - Use `proptest` to generate resources where `describe()` returns divergent state from persisted state
    - Verify `delete()` receives the live state from `describe()`, not the persisted state
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

- [ ] 14. Verify config writeback (existing — no code changes)
  - [ ] 14.1 Verify existing `write_tokeirad_writeback` behavior
    - Audit `write_tokeirad_writeback` in `apps/tkr/src/commands/infra.rs` to confirm:
      - Writeback values are collected from the deployment after apply
      - Non-empty values are written to the config file using dotted-key TOML insertion
      - Intermediate tables are created when key paths don't exist
      - Existing values are overwritten
      - Errors are propagated
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [ ]* 14.2 Write property test for TOML writeback round-trip (Property 8)
    - **Property 8: TOML Writeback Round-Trip**
    - **Validates: Requirements 5.2, 5.3, 5.4, 5.6**
    - Use `proptest` to generate random sets of dotted-key/value pairs (valid TOML paths, non-empty string values)
    - Write them to a TOML document, read back each value at its path
    - Verify all values match the originals
    - Minimum 100 iterations
    - Test location: `apps/tkr/src/commands/infra.rs` `#[cfg(test)]` module

- [ ] 15. Final checkpoint — Ensure all tests pass
  - Run `cargo clippy --workspace --all-targets` and `cargo test --workspace` and verify all tests pass including property tests, unit tests, and mode-aware engine logic.

## Notes

- All tests are required — property tests are marked `*` per convention but are mandatory per project rules.
- `ResourceMode` defaults to `Managed` via `#[serde(default)]` for backward compatibility with existing state files.
- `mode()` is a required `Resource` trait method — all 12 `tokeira-aws` resources, both platform crates, and `tokeira-compose` must be updated.
- `console` and `indicatif` are added only to the `tkr` binary crate, not to library crates.
- Config writeback is already implemented — task 14 formalizes and tests the existing behavior.
- Property tests use `proptest` with minimum 100 iterations per the project convention.
- All code must pass `cargo clippy --workspace --all-targets` per AGENTS.md.
