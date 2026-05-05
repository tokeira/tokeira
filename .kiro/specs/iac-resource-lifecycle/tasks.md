# Implementation Plan: IaC Resource Lifecycle

## Overview

Extend `tokeira-iac` with a `DestroyMode` marker and formalise existing engine invariants (incremental state saves, module-scoped delete filtering, describe-before-delete, config writeback) with property-based tests. Add CLI progress reporting via `indicatif` and `console` to the `tkr` binary. Document the `effective_managed` convention for resources that need multiple lifecycle modes.

Crucially, this plan does **not** add a generic `ResourceMode` enum, a `Resource::mode()` trait method, or a `ResourceState.mode` field. Mode-awareness belongs in each resource that needs it.

Target crates:
- `tokeira-iac` — add `DestroyMode`, formalise existing behavior with tests
- `tokeira-orchestrator` — register `DestroyMode` in destroy operations
- `apps/tkr` — CLI progress reporting (`indicatif`, `console`)

## Tasks

- [ ] 1. Add `DestroyMode` marker to `tokeira-iac`
  - [ ] 1.1 Define the `DestroyMode` struct
    - Add `#[derive(Debug, Clone, Copy)] pub struct DestroyMode;` to `crates/tokeira-iac/src/lib.rs`
    - Export from crate root
    - Add a doc comment explaining its role: registered in `ProvisionContext` during destroy, inspected by modules via `ModuleContext::extension::<DestroyMode>()`
    - _Requirements: 1.1_

  - [ ]* 1.2 Write property test for `DestroyMode` visibility through `ModuleContext` (Property 1)
    - **Property 1: DestroyMode Visibility**
    - **Validates: Requirements 1.3, 1.4**
    - Construct a `ProvisionContext`, optionally call `set_extension(DestroyMode)`, construct a `ModuleContext` from its state and extensions
    - Assert `ctx.extension::<DestroyMode>().is_some()` iff the marker was set
    - Use `proptest` with a bool to randomise registration; minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/module.rs` `#[cfg(test)]` module

- [ ] 2. Wire `DestroyMode` registration in the orchestrator
  - [ ] 2.1 Register `DestroyMode` in `InfraEngine::destroy` and `InfraEngine::plan_destroy`
    - In `crates/tokeira-orchestrator/src/lib.rs` (or the appropriate orchestrator file), call `self.ctx.set_extension(DestroyMode)` before calling `engine.destroy_modules` or `engine.plan_destroy_modules`
    - Do NOT register during `plan`, `apply`, or any other operation
    - Import `DestroyMode` from `tokeira_iac`
    - _Requirements: 1.2, 1.5_

  - [ ]* 2.2 Write unit test for `DestroyMode` registration
    - Verify `ctx.extension::<DestroyMode>()` is `Some` after `InfraEngine::destroy` is called
    - Verify `ctx.extension::<DestroyMode>()` is `Some` after `InfraEngine::plan_destroy` is called
    - Verify `ctx.extension::<DestroyMode>()` is `None` after `InfraEngine::apply` or `InfraEngine::plan`
    - Test location: `crates/tokeira-orchestrator/src/lib.rs` `#[cfg(test)]` module
    - _Requirements: 1.2, 1.5_

- [ ] 3. Checkpoint — Ensure workspace compiles
  - Run `cargo clippy --workspace --all-targets` and verify the workspace compiles cleanly with the new `DestroyMode` marker.

- [ ] 4. Formalise the `StateSaver` incremental-save contract
  - [ ] 4.1 Audit engine for `StateSaver` invocation points
    - In `crates/tokeira-iac/src/engine.rs`, verify `apply_changes` calls `saver(&ctx.state).await?` exactly once after each successful create, update, and delete
    - Verify `destroy_changes` calls the saver after each delete and after each state-only prune
    - Verify `refresh_state` calls the saver after pruning stale resources (when `has_managed_missing` is true)
    - Add comments citing the spec requirements where the invariant is relied on
    - _Requirements: 2.1, 2.2, 2.3, 2.5_

  - [ ] 4.2 Audit engine for `StateSaver` error propagation
    - Verify `save(&ctx.state).await?` propagates errors immediately via the `?` operator, without swallowing or batching
    - _Requirements: 2.4_

  - [ ]* 4.3 Write property test for `StateSaver` invocation count (Property 2)
    - **Property 2: StateSaver Invocation Count**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.6**
    - Use `proptest` to generate random sequences of N resources with a mix of create/update/delete changes
    - Use an `Arc<AtomicUsize>` counter in the `StateSaver` callback
    - After apply completes, assert `counter == N` (where N is the number of non-NoChange operations)
    - Use a `FakeResource` type defined in the engine test module
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

  - [ ]* 4.4 Write property test for `StateSaver` error aborts engine (Property 3)
    - **Property 3: StateSaver Error Aborts Engine**
    - **Validates: Requirements 2.4**
    - Generate N operations, pick a random K ∈ [1, N], configure the saver to fail on invocation K
    - Use an atomic counter to track completed mutations
    - Assert apply returns `Err` and `completed ≤ K`
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

- [ ] 5. Formalise module-scoped delete filtering
  - [ ] 5.1 Audit `filter_changes_by_modules` behavior
    - In `crates/tokeira-iac/src/engine.rs`, verify the function suppresses Delete changes when the resource's persisted module is not in the active set
    - Verify it preserves all Create, Update, and NoChange entries unchanged
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [ ]* 5.2 Write property test for module-scoped delete filtering (Property 4)
    - **Property 4: Module-Scoped Delete Filtering**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6**
    - Generate random sets of `Change` entries (mix of Create, Update, Delete, NoChange) with random module assignments
    - Generate random active module sets (including empty, single-module, and multi-module cases)
    - After filtering, assert: every Delete in the output has a persisted module in the active set
    - Assert: every Create, Update, NoChange from the input is present in the output (preservation)
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

- [ ] 6. Formalise describe-before-delete safety
  - [ ] 6.1 Audit destroy_changes for describe-before-delete
    - In `crates/tokeira-iac/src/engine.rs`, verify `destroy_changes` calls `resource.describe(ctx).await?` before `resource.delete(live, ctx).await?`
    - Verify when describe returns `None`, the resource is pruned from state without calling `delete()`
    - Verify when describe returns `Some(live_state)`, `live_state` is passed to `delete()` (not the stale persisted state)
    - Verify describe errors propagate immediately
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [ ]* 6.2 Write property test for describe-before-delete count (Property 7)
    - **Property 7: Describe-Before-Delete Count**
    - **Validates: Requirements 6.1, 6.2, 6.5**
    - Generate N resources in state, configure K of them to have `describe()` return `None`
    - Use an atomic counter in a `FakeResource::delete()` override
    - After destroy, assert `delete_call_count ≤ N - K`
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

  - [ ]* 6.3 Write property test for describe-before-delete uses live state (Property 8)
    - **Property 8: Describe-Before-Delete Uses Live State**
    - **Validates: Requirements 6.3**
    - Configure `FakeResource::describe()` to return a state divergent from the persisted state
    - Configure `FakeResource::delete()` to capture the `current: &ResourceState` argument
    - Assert the captured state equals the describe result, not the persisted state
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

- [ ] 7. Checkpoint — Ensure engine tests pass
  - Run `cargo test -p tokeira-iac` and `cargo clippy --workspace --all-targets` and verify all tests pass.

- [ ] 8. Implement CLI progress reporting with `indicatif` and `console`
  - [ ] 8.1 Add `console` and `indicatif` dependencies to `apps/tkr`
    - Add `console = "0.15"` and `indicatif = "0.17"` (or the current workspace versions) to `apps/tkr/Cargo.toml`
    - These are binary-crate-only dependencies — do NOT add to any library crate
    - _Requirements: 5.1, 5.2, 5.3_

  - [ ] 8.2 Create the `tui` module in `apps/tkr`
    - Create `apps/tkr/src/tui.rs`
    - Define `OutputFormat` enum (`Human`, `Json`) with `Debug, Clone, Copy, PartialEq, Eq`
    - Define `ActionCounters` struct with three `AtomicUsize` fields (`completed`, `failed`, `skipped`)
    - Define `ActionTuiHandle` struct with fields: `format: OutputFormat`, `multi: MultiProgress`, `start: Instant`, `counters: Arc<ActionCounters>`, `is_terminal: bool`
    - Implement `ActionTuiHandle::new(format)` that detects TTY via `console::Term::stdout().is_term()` and initialises counters
    - Define `ProgressEvent` enum with `serde::Serialize` and `#[serde(tag = "event", rename_all = "snake_case")]`: `OperationStart`, `WaitProgress`, `Note`, `Summary`
    - Add `mod tui;` to `apps/tkr/src/main.rs`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.9_

  - [ ] 8.3 Implement `ActionTuiHandle::install` for all three progress reporters
    - Clone `format`, `multi`, `counters`, `is_terminal` into the closures
    - Use atomic counters to update completed/failed/skipped
    - `set_apply_progress`: for `Human` + terminal, add a spinner via `multi.add(ProgressBar::new_spinner())` with a template; for `Human` + non-terminal, `eprintln!` a plain line; for `Json`, emit `ProgressEvent::OperationStart` as one JSON line to stdout
    - `set_wait_progress`: similar branching; for `Human`, update a spinner message with elapsed/timeout; for `Json`, emit `ProgressEvent::WaitProgress`
    - `set_note_progress`: similar branching; for `Human`, print a note line; for `Json`, emit `ProgressEvent::Note`
    - Each JSON event SHALL be serialisable via `serde_json::to_string` without panicking
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.9_

  - [ ] 8.4 Implement `ActionTuiHandle::print_summary`
    - Read the three counters with `Ordering::Relaxed`
    - For `Human`: print a styled summary line using `console::style("Done:").bold()` with counts and elapsed seconds
    - For `Json`: emit `ProgressEvent::Summary` as one JSON line
    - _Requirements: 5.8_

  - [ ] 8.5 Wire `ActionTuiHandle` into `infra apply`, `infra destroy`, and `infra plan` commands
    - In `apps/tkr/src/commands/infra.rs`, construct `ActionTuiHandle::new(format)` where `format` comes from the `--json` flag
    - Call `tui.install(&mut ctx)` before invoking `engine.plan/apply/destroy`
    - Call `tui.print_summary()` after the engine returns (success or failure)
    - _Requirements: 5.1, 5.2, 5.3, 5.8_

  - [ ]* 8.6 Write property test for JSON event well-formedness (Property 9)
    - **Property 9: JSON Progress Event Well-Formedness**
    - **Validates: Requirements 5.7**
    - Generate random `ProgressEvent` variants with random field values
    - Serialise each via `serde_json::to_string`, then parse via `serde_json::from_str` back into `ProgressEvent`
    - Assert the parsed event equals the original
    - Minimum 100 iterations
    - Test location: `apps/tkr/src/tui.rs` `#[cfg(test)]` module

  - [ ]* 8.7 Write property test for progress counter accuracy (Property 10)
    - **Property 10: Progress Counter Accuracy**
    - **Validates: Requirements 5.8**
    - Simulate N progress events with a mix of completion, failure, and skip outcomes
    - After all events, assert `counters.completed.load() + counters.failed.load() + counters.skipped.load() == N`
    - Minimum 100 iterations
    - Test location: `apps/tkr/src/tui.rs` `#[cfg(test)]` module

  - [ ]* 8.8 Write unit tests for terminal vs non-terminal fallback
    - Construct `ActionTuiHandle` with `is_terminal = false` and verify the `Human` path emits plain lines (no ANSI escapes)
    - Construct with `is_terminal = true` and verify the spinner path is used
    - Test location: `apps/tkr/src/tui.rs` `#[cfg(test)]` module
    - _Requirements: 5.9_

- [ ] 9. Checkpoint — Ensure CLI compiles and tests pass
  - Run `cargo clippy --workspace --all-targets` and `cargo test -p tkr` and verify the CLI compiles cleanly with the new `tui` module.

- [ ] 10. Formalise config writeback behavior
  - [ ] 10.1 Audit `write_tokeirad_writeback`
    - In `apps/tkr/src/commands/infra.rs`, verify the function uses `toml_edit::DocumentMut` (preserves comments and formatting)
    - Verify intermediate tables are created via `item[part] = toml_edit::Item::Table(toml_edit::Table::new())` when they don't exist
    - Verify existing values are overwritten via `item[last_part] = toml_edit::value(value)`
    - Verify write errors are propagated
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [ ]* 10.2 Write property test for TOML writeback round-trip (Property 5)
    - **Property 5: TOML Writeback Round-Trip**
    - **Validates: Requirements 4.2, 4.3, 4.4, 4.7**
    - Generate random sets of (dotted_key, value) pairs where keys are valid TOML identifier paths (alphanumeric + underscore, segment count 1..=5) and values are non-empty strings
    - Start from an empty TOML document, apply the writeback, then read each key back
    - Assert each read value equals the original
    - Minimum 100 iterations
    - Test location: `apps/tkr/src/commands/infra.rs` `#[cfg(test)]` module

  - [ ]* 10.3 Write property test for comment preservation (Property 6)
    - **Property 6: TOML Writeback Preserves Comments**
    - **Validates: Requirements 4.5**
    - Generate a TOML document with random comments (both line comments `#` and end-of-line comments)
    - Apply a writeback operation that modifies values
    - Assert every original comment is still present in the output
    - Minimum 100 iterations
    - Test location: `apps/tkr/src/commands/infra.rs` `#[cfg(test)]` module

- [ ] 11. Document the `effective_managed` convention
  - [ ] 11.1 Add a doc-comment module in `tokeira-iac` describing the convention
    - Add `crates/tokeira-iac/src/resource_modes.rs` as a documentation-only module (no executable code)
    - Include the convention pattern with the DSQL cluster example from the design document
    - Describe: how to define a mode enum, how to persist mode in `properties["mode"]`, how to implement `effective_managed` in `delete()`
    - Export nothing — this is purely reference documentation
    - Reference this module from the crate root doc comment
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6_

  - [ ] 11.2 Verify the DSQL cluster resource follows the convention
    - In `crates/tokeira-aws/src/resources/dsql_cluster.rs`, verify `DsqlClusterMode` has `Managed` and `Preexisting` variants
    - Verify `create()` persists `"mode": "managed"` or `"mode": "preexisting"` in `ResourceState.properties`
    - Verify `delete()` computes `effective_managed = self.config.mode == DsqlClusterMode::Managed || state_mode == "managed"` and skips the provider call if false
    - Add a regression test: config `Preexisting`, persisted state `"mode": "managed"` → `delete()` must call the provider API
    - _Requirements: 7.2, 7.4, 7.6_

- [ ] 12. Final checkpoint — Ensure all tests pass
  - Run `cargo clippy --workspace --all-targets` to verify code quality
  - Run `cargo test --workspace` and verify all tests pass, including property tests, unit tests, engine invariants, and the DSQL cluster convention regression test
  - Run `cargo +nightly fmt --all --check` to verify formatting

## Notes

- All tests are required — property tests are marked `*` per convention but are mandatory per project rules.
- No generic `ResourceMode` enum, no `Resource::mode()` trait method, no `ResourceState.mode` field. Mode-awareness is resource-specific.
- Resources that need lifecycle variation follow the `effective_managed` convention documented in task 11.1. The DSQL cluster resource is the reference implementation.
- `console` and `indicatif` are added only to the `tkr` binary crate, not to library crates.
- Config writeback is already implemented in `tokeira-orchestrator` and `apps/tkr` — tasks 10.x formalise and test the existing behavior.
- Module-scoped delete filtering, describe-before-delete, and `StateSaver` incremental saves are already implemented in `tokeira-iac` — tasks 4–6 formalise and test the existing behavior.
- Property tests use `proptest` with minimum 100 iterations per the project convention.
- All code must pass `cargo clippy --workspace --all-targets` per AGENTS.md.
