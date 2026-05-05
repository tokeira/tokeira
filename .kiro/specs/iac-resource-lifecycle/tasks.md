# Implementation Plan: IaC Resource Lifecycle

## Overview

Extend `tokeira-iac` with a `DestroyMode` marker, a new `plan_destroy` engine method, CAS-aware incremental state saves, describe-before-delete in destroy paths, and expanded progress callbacks (start/complete/failed). Add CLI progress reporting via `indicatif` and `console` to the `tkr` binary, threading `--json` from `Cli` into `commands::infra::run`. Document the `effective_managed` convention for resources that need multiple lifecycle modes.

Crucially, this plan does **not** add a generic `ResourceMode` enum, a `Resource::mode()` trait method, or a `ResourceState.mode` field. Mode-awareness belongs in each resource that needs it.

Target crates:
- `tokeira-iac` — add `DestroyMode`, `ProvisionContext::remove_extension`, `Engine::plan_destroy`, completion/failure progress callbacks, per-resource refresh prune saves
- `tokeira-orchestrator` — register `DestroyMode`, remove after operation, CAS version tracking across incremental saves, remove final state save; `StateStore::save` returns the new version
- `apps/tkr` — CLI progress reporting (`indicatif`, `console`), thread `cli.json` through `Command::Infra` handler

## Tasks

- [ ] 1. Add `DestroyMode` marker and `remove_extension` to `tokeira-iac`
  - [ ] 1.1 Define `DestroyMode` and extend `ProvisionContext` API
    - Add `#[derive(Debug, Clone, Copy)] pub struct DestroyMode;` to `crates/tokeira-iac/src/lib.rs`; export from crate root with a doc comment explaining its role
    - Add `pub fn remove_extension<T: Send + Sync + 'static>(&mut self) -> bool` to `ProvisionContext`; returns `true` when an extension of the requested type was present and removed
    - `ModuleContext` already exposes extensions through `extension::<T>()`; confirm no additional change is required there
    - _Requirements: 1.1, 1.6_

  - [ ]* 1.2 Write property test for `DestroyMode` visibility through `ModuleContext` (Property 1)
    - **Property 1: DestroyMode Visibility**
    - **Validates: Requirements 1.3, 1.4**
    - Construct a `ProvisionContext`, optionally call `set_extension(DestroyMode)`, construct a `ModuleContext` from its state and extensions
    - Assert `ctx.extension::<DestroyMode>().is_some()` iff the marker was set
    - Use `proptest` with a bool to randomise registration; minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/module.rs` `#[cfg(test)]` module

  - [ ]* 1.3 Write unit test for `ProvisionContext::remove_extension`
    - Set a marker, assert `remove_extension::<DestroyMode>()` returns `true` and `extension::<DestroyMode>()` returns `None` afterwards
    - Call `remove_extension::<DestroyMode>()` on an empty context, assert it returns `false`
    - Test location: `crates/tokeira-iac/src/lib.rs` `#[cfg(test)]` module
    - _Requirements: 1.6_

- [ ] 2. Add `Engine::plan_destroy` and wire orchestrator destroy paths
  - [ ] 2.1 Implement `Engine::plan_destroy` and `Engine::plan_destroy_for_modules` in `tokeira-iac`
    - Add `pub async fn plan_destroy(&self, composition: &InfraComposition, ctx: &mut ProvisionContext) -> Result<Vec<Change>, IacError>` and `pub async fn plan_destroy_for_modules(&self, composition: &InfraComposition, ctx: &mut ProvisionContext) -> Result<Vec<Change>, IacError>` to `crates/tokeira-iac/src/engine.rs`
    - Both methods collect `known_modules`, call `refresh_state(&known, &[], ctx)` (no saver — refresh mutates `ctx.state` in memory only), compute the zero-desired change set, and (for `_for_modules`) apply `filter_changes_by_modules` using `composition.active_modules`
    - Do not call any `Resource::create`, `update`, or `delete` — `plan_destroy` is read-only; the only side effect is updating `ctx.state` in memory
    - _Requirements: 1.2, 2.5, 3.1_

  - [ ] 2.2 Register `DestroyMode` in the orchestrator and scope it to the operation
    - In `crates/tokeira-orchestrator/src/lib.rs`, change `InfraEngine::destroy` to accept a `ModuleSelection` parameter. Branch on the selection: `ModuleSelection::All` calls `engine.destroy(composition, &mut ctx, Some(&saver))`; `ModuleSelection::Only(_) | ModuleSelection::Except(_)` both call `engine.destroy_for_modules(composition, &mut ctx, Some(&saver))`. Both filtered variants resolve to `composition.active_modules` inside `compose()`, so the engine call is the same. Do NOT gate on `composition.active_modules.is_empty()` — `compose()` always populates that list even for `All`
    - Set `DestroyMode` on `self.ctx` before the engine call; call `self.ctx.remove_extension::<DestroyMode>()` after the engine returns (success or error)
    - Add `InfraEngine::plan_destroy(&mut self, composition, selection)` that loads state, sets `DestroyMode`, dispatches to `engine.plan_destroy` for `All` or `engine.plan_destroy_for_modules` for `Only(_)`/`Except(_)`, then removes the marker
    - Do NOT register `DestroyMode` during `plan` or `apply`
    - Import `DestroyMode` and `ModuleSelection` from `tokeira_iac`
    - Update all call sites (e.g., `commands::infra::run`) to pass `ModuleSelection` through to `destroy`/`plan_destroy`
    - _Requirements: 1.2, 1.5, 1.6_

  - [ ]* 2.3 Write unit test for `DestroyMode` registration and scoping
    - Verify `ctx.extension::<DestroyMode>()` is `Some` during `InfraEngine::destroy` and `InfraEngine::plan_destroy`
    - Verify `ctx.extension::<DestroyMode>()` is `None` after both methods return (success path)
    - Verify `ctx.extension::<DestroyMode>()` is `None` after `InfraEngine::apply` or `InfraEngine::plan`
    - Verify that if the engine call returns an error, `DestroyMode` has still been removed
    - Verify that `ModuleSelection::All` dispatches to `engine.destroy`/`plan_destroy` and both `ModuleSelection::Only(_)` and `ModuleSelection::Except(_)` dispatch to `destroy_for_modules`/`plan_destroy_for_modules`
    - Test location: `crates/tokeira-orchestrator/src/lib.rs` `#[cfg(test)]` module
    - _Requirements: 1.2, 1.5, 1.6_

- [ ] 3. Checkpoint — Ensure workspace compiles
  - Run `cargo lint` and `cargo check --workspace` and verify the workspace compiles cleanly with the new `DestroyMode`, `remove_extension`, and `plan_destroy` APIs

- [ ] 4. Implement CAS-aware incremental state saving
  - [ ] 4.1 Update `StateStore::save` to return the new version
    - In `crates/tokeira-state/src/lib.rs` (or equivalent module owning `StateStore`), change `save` to return `Result<String, StateError>` where the returned string is the new ETag/version
    - Update `CasStore` and `S3StateStore` implementations to return the post-write version from the underlying CAS write
    - Update all call sites in the workspace to consume the returned version
    - _Requirements: 2a.2_

  - [ ] 4.2 Rewrite `make_saver` in `tokeira-orchestrator` for CAS tracking
    - Replace the captured `version: String` with `Arc<Mutex<String>>` (tokio `Mutex` since the saver is async) seeded with the initial version loaded from the store
    - Inside the closure, lock the mutex to read the current version, call `store.save(&state, &current)`, then update the mutex with the returned new version before releasing the lock
    - Propagate errors immediately from `store.save` (including CAS conflicts) without retry
    - _Requirements: 2a.1, 2a.4, 2a.5_

  - [ ] 4.3 Remove the final `state_store.save` call from `apply` and `destroy`
    - Delete the trailing `let _ = self.state_store.save(&self.ctx.state, &version).await?;` lines in `InfraEngine::apply` and `InfraEngine::destroy`
    - All saves now flow through the incremental saver
    - _Requirements: 2a.3_

  - [ ] 4.4 Save per pruned resource during `refresh_state` (when a saver is available)
    - In `crates/tokeira-iac/src/engine.rs`, change the `refresh_state` loop that removes stale resources so it invokes the `StateSaver` once per pruned resource rather than once after the loop completes
    - Gate the save call on `if let Some(save) = saver` so that plan paths — which call `refresh_state` without a saver — mutate `ctx.state` in memory only and do not attempt to persist. Apply and destroy paths always supply a saver, so every prune persists immediately
    - Thread `saver: Option<&StateSaver>` through `refresh_state`'s signature so the function can branch on its presence
    - Remove the `has_managed_missing` single-save block in `apply_with_known`, `apply_for_modules`, `destroy_known`, and `destroy_for_modules` — the per-prune save subsumes it
    - _Requirements: 2.5, 2.6_

  - [ ] 4.5 Audit `apply_changes` and `destroy_changes` saver invocations
    - Verify `apply_changes` calls the saver exactly once after each successful create, update, and delete (existing behavior — confirm and comment)
    - Verify `destroy_changes` calls the saver after each delete and after each state-only prune
    - Verify errors from the saver propagate immediately via `?`
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [ ]* 4.6 Write property test for `StateSaver` invocation count (Property 2)
    - **Property 2: StateSaver Invocation Count**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.6**
    - Use `proptest` to generate random sequences of N resources with a mix of create/update/delete changes
    - Use an `Arc<AtomicUsize>` counter in the `StateSaver` callback
    - After apply completes, assert `counter == N` (where N is the number of non-NoChange operations)
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

  - [ ]* 4.7 Write property test for `StateSaver` error aborts engine (Property 3)
    - **Property 3: StateSaver Error Aborts Engine**
    - **Validates: Requirements 2.4**
    - Generate N operations, pick a random K ∈ [1, N], configure the saver to fail on invocation K
    - Assert apply returns `Err` and the completed-mutation counter `≤ K`
    - Minimum 100 iterations
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module

  - [ ]* 4.8 Write unit test for CAS version tracking across saves
    - Construct an orchestrator with a fake `StateStore` whose `save` returns a monotonically increasing version (`"v1"`, `"v2"`, …) and asserts the caller always passes the previously-returned version
    - Trigger three incremental saves via an apply over three resources; assert no CAS conflict and the final tracked version is `"v3"`
    - Test location: `crates/tokeira-orchestrator/src/lib.rs` `#[cfg(test)]` module
    - _Requirements: 2a.1, 2a.2_

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
  - [ ] 6.1 Audit `destroy_changes` for describe-before-delete
    - In `crates/tokeira-iac/src/engine.rs`, verify `destroy_changes` calls `resource.describe(ctx).await?` before `resource.delete(live, ctx).await?`
    - Verify when describe returns `None`, the resource is pruned from state without calling `delete()` and the saver is invoked once for that prune
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
  - Run `cargo test -p tokeira-iac -p tokeira-orchestrator` and `cargo lint` and verify all tests pass

- [ ] 8. Extend `ProvisionContext` with completion/failure progress callbacks
  - [ ] 8.1 Add new progress callback fields and setters to `ProvisionContext`
    - In `crates/tokeira-iac/src/lib.rs`, add callback fields for `CompleteProgressReporter` and `FailedProgressReporter` alongside the existing apply/wait/note reporters
    - Add signatures:
      - `set_complete_progress<F>(&mut self, reporter: F)` where `F: Fn(&str, &ResourceId, &ResourceType, Duration) + Send + Sync + 'static`
      - `set_failed_progress<F>(&mut self, reporter: F)` where `F: Fn(&str, &ResourceId, &ResourceType, Duration, &IacError) + Send + Sync + 'static`
      - `emit_complete_progress(&self, action: &str, id: &ResourceId, rtype: &ResourceType, elapsed: Duration)`
      - `emit_failed_progress(&self, action: &str, id: &ResourceId, rtype: &ResourceType, elapsed: Duration, err: &IacError)`
    - _Requirements: 5.1, 5.4, 5.5_

  - [ ] 8.2 Call completion/failure callbacks from `apply_changes` and `destroy_changes`
    - In `crates/tokeira-iac/src/engine.rs`, around each `resource.create/update/delete` call in `apply_changes` and around each `resource.delete` call in `destroy_changes`, record `let started = Instant::now();` before the call and invoke `emit_complete_progress(..., started.elapsed())` on the `Ok` branch or `emit_failed_progress(..., started.elapsed(), &err)` on the `Err` branch before propagating
    - Also call `emit_note_progress` when a describe-prune occurs in `destroy_changes` so the operator sees "pruned absent: {id}"
    - _Requirements: 5.3, 5.4, 5.5_

  - [ ]* 8.3 Write unit tests for new callbacks
    - Using a stub resource with configurable success/failure, install mock closures that push events into a shared `Vec`
    - Drive `apply_changes` and assert `OperationStart` → `OperationComplete` pairs with monotonic elapsed durations
    - Drive a failing resource and assert `OperationStart` → `OperationFailed` with the expected error message
    - Test location: `crates/tokeira-iac/src/engine.rs` `#[cfg(test)]` module
    - _Requirements: 5.3, 5.4, 5.5_

- [ ] 9. Implement CLI progress reporting with `indicatif` and `console`
  - [ ] 9.1 Add `console` and `indicatif` dependencies to `apps/tkr`
    - Add `console = "0.15"` and `indicatif = "0.17"` to `apps/tkr/Cargo.toml`
    - These are binary-crate-only dependencies — do NOT add to any library crate
    - Ensure `serde_json` is already available (it is transitively; confirm)
    - _Requirements: 5.6, 5.7, 5.8_

  - [ ] 9.2 Create the `tui` module in `apps/tkr`
    - Create `apps/tkr/src/tui.rs`
    - Define `OutputFormat` enum (`Human`, `Json`) with `Debug, Clone, Copy, PartialEq, Eq`
    - Define `ActionCounters` struct with three `AtomicUsize` fields (`completed`, `failed`, `skipped`)
    - Define `ActiveSpinners` struct with `entries: Mutex<HashMap<ResourceId, SpinnerEntry>>`; each `SpinnerEntry` carries `started_at: Instant` and `bar: Option<ProgressBar>`
    - Define `ActionTuiHandle` with fields: `format`, `multi: MultiProgress`, `start: Instant`, `counters: Arc<ActionCounters>`, `spinners: Arc<ActiveSpinners>`, `is_terminal: bool`
    - Implement `ActionTuiHandle::new(format)` that detects TTY via `console::Term::stdout().is_term()`
    - Implement `pub(crate) fn with_terminal_detected(format: OutputFormat, is_terminal: bool) -> Self` as a test-only constructor used by unit tests to force terminal vs non-terminal paths without depending on the runner TTY
    - Implement `pub fn record_skipped(&self, n: usize)` that stores `n` into `counters.skipped` — the CLI calls this with the count of `ChangeKind::NoChange` entries from the engine's plan result
    - Implement a module-private `fn emit_json_line(event: &ProgressEvent)` helper that serialises via `serde_json::to_string(event)`, prints one line on `Ok`, and logs via `tracing::warn!` on `Err`. No `.unwrap()` or `.expect()` in production code
    - Define `ProgressEvent` enum deriving `Serialize, Deserialize, PartialEq, Eq` with `#[serde(tag = "event", rename_all = "snake_case")]` and variants `OperationStart`, `OperationComplete`, `OperationFailed`, `WaitProgress`, `Note`, `Summary`
    - Add `mod tui;` to `apps/tkr/src/main.rs`
    - _Requirements: 5.6, 5.7, 5.11, 5.12, 5.13_

  - [ ] 9.3 Implement `ActionTuiHandle::install` wiring all five reporters
    - Clone `format`, `multi`, `counters`, `spinners`, `is_terminal` into each closure (closures are `Fn`, not `FnMut`; use interior mutability)
    - `set_apply_progress`: insert a new `SpinnerEntry` keyed by `ResourceId` with the current `Instant` and (for `Human`+TTY) a spinner added via `multi.add(ProgressBar::new_spinner())`; for `Human`+non-TTY print an `eprintln!` start line; for `Json` emit `OperationStart` via the `emit_json_line` helper
    - `set_complete_progress`: look up and remove the entry, finish the spinner with `✓ {rid} ({elapsed})`, increment `counters.completed`; for `Human`+non-TTY print a plain line; for `Json` emit `OperationComplete` via `emit_json_line`
    - `set_failed_progress`: look up and remove the entry, finish the spinner with `✗ {rid} ({elapsed}): {err}`, increment `counters.failed`; for `Human`+non-TTY print a plain line; for `Json` emit `OperationFailed` via `emit_json_line`
    - `set_wait_progress`: for `Human` update the spinner's message with elapsed/timeout; for `Json` emit `WaitProgress` via `emit_json_line`
    - `set_note_progress`: for `Human` print a dim note line under the active spinner; for `Json` emit `Note` via `emit_json_line`
    - All JSON emission SHALL route through the `emit_json_line(&ProgressEvent)` helper defined in `tui.rs`: it calls `serde_json::to_string(&event)`, writes the line on `Ok`, and logs via `tracing::warn!` on `Err`. NO use of `.unwrap()` or `.expect()` in non-test code per AGENTS.md
    - _Requirements: 5.1, 5.2, 5.3, 5.6, 5.7, 5.8, 5.9, 5.10, 5.11_

  - [ ] 9.4 Implement `ActionTuiHandle::print_summary`
    - Read the three counters with `Ordering::Relaxed`
    - For `Human`: print a styled summary line using `console::style("Done:").bold()` with counts and elapsed seconds
    - For `Json`: emit `ProgressEvent::Summary` as one JSON line
    - _Requirements: 5.12_

  - [ ] 9.5 Thread `cli.json` into `commands::infra::run` and wire the TUI
    - In `apps/tkr/src/cli.rs`, if `Cli::json` is not already a global flag, add `#[arg(long, global = true)]` `json: bool`
    - In `apps/tkr/src/main.rs`, update the `Command::Infra { action }` arm to pass `cli.json` (as `OutputFormat`) to `commands::infra::run(action, &deployments, ctx, format)`
    - In `apps/tkr/src/commands/infra.rs`, update `run` to accept `format: OutputFormat`, construct `ActionTuiHandle::new(format)` once, call `tui.install(&mut ctx)` before each engine call, and call `tui.print_summary()` after the engine returns (success or error)
    - After the engine returns, compute `let skipped = changes.iter().filter(|c| matches!(c.kind, ChangeKind::NoChange)).count();` and call `tui.record_skipped(skipped)` before `print_summary` so the summary reflects unchanged resources
    - _Requirements: 5.12, 5.14_

  - [ ]* 9.6 Write property test for JSON event well-formedness (Property 9)
    - **Property 9: JSON Progress Event Well-Formedness**
    - **Validates: Requirements 5.11**
    - Generate random `ProgressEvent` variants with random field values
    - Serialise each via `serde_json::to_string`, parse via `serde_json::from_str` back into `ProgressEvent`
    - Assert the parsed event equals the original
    - Minimum 100 iterations
    - Test location: `apps/tkr/src/tui.rs` `#[cfg(test)]` module

  - [ ]* 9.7 Write property test for progress counter accuracy (Property 10)
    - **Property 10: Progress Counter Accuracy**
    - **Validates: Requirements 5.12**
    - Generate triples `(completed_events, failed_events, noChange_count)` with N = completed + failed + noChange
    - For each completed/failed event invoke the matching installed closure (`complete_progress` or `failed_progress`) directly; call `tui.record_skipped(noChange_count)` once
    - After all events, assert `counters.completed.load() + counters.failed.load() + counters.skipped.load() == N`
    - Minimum 100 iterations
    - Test location: `apps/tkr/src/tui.rs` `#[cfg(test)]` module

  - [ ]* 9.8 Write unit tests for terminal vs non-terminal fallback
    - Construct `ActionTuiHandle::with_terminal_detected(OutputFormat::Human, false)` and drive a start/complete cycle; verify via a captured writer trait (or behavioral observation such as spinner-handle presence in the `ActiveSpinners` map) that the `Human` path emits plain lines with no spinner attached
    - Construct `ActionTuiHandle::with_terminal_detected(OutputFormat::Human, true)` and verify the start callback inserts a `SpinnerEntry { bar: Some(_), .. }` into `spinners.entries`
    - Prefer behavioural assertions (entry presence, counter values) over capturing stdout, because `indicatif` and `console` write through their own layers; if stdout capture is required, wire it via `MultiProgress::with_draw_target(ProgressDrawTarget::hidden())` in tests so output does not reach the runner
    - Test location: `apps/tkr/src/tui.rs` `#[cfg(test)]` module
    - _Requirements: 5.13_

- [ ] 10. Checkpoint — Ensure CLI compiles and tests pass
  - Run `cargo lint` and `cargo test -p tkr` and verify the CLI compiles cleanly with the new `tui` module and the threaded `--json` flag

- [ ] 11. Formalise config writeback behavior
  - [ ] 11.1 Audit `write_tokeirad_writeback`
    - In `apps/tkr/src/commands/infra.rs`, verify the function uses `toml_edit::DocumentMut` (preserves comments and formatting)
    - Verify intermediate tables are created via `item[part] = toml_edit::Item::Table(toml_edit::Table::new())` when they don't exist
    - Verify existing values are overwritten via `item[last_part] = toml_edit::value(value)`
    - Verify write errors are propagated
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [ ]* 11.2 Write property test for TOML writeback round-trip (Property 5)
    - **Property 5: TOML Writeback Round-Trip**
    - **Validates: Requirements 4.2, 4.3, 4.4, 4.7**
    - Generate random sets of (dotted_key, value) pairs where keys are valid TOML identifier paths (alphanumeric + underscore, segment count 1..=5) and values are non-empty strings
    - Start from an empty TOML document, apply the writeback, then read each key back
    - Assert each read value equals the original
    - Minimum 100 iterations
    - Test location: `apps/tkr/src/commands/infra.rs` `#[cfg(test)]` module

  - [ ]* 11.3 Write property test for comment preservation (Property 6)
    - **Property 6: TOML Writeback Preserves Comments**
    - **Validates: Requirements 4.5**
    - Generate a TOML document with random comments (both line comments `#` and end-of-line comments)
    - Apply a writeback operation that modifies values
    - Assert every original comment is still present in the output
    - Minimum 100 iterations
    - Test location: `apps/tkr/src/commands/infra.rs` `#[cfg(test)]` module

- [ ] 12. Document and verify the `effective_managed` convention
  - [ ] 12.1 Add a doc-only module in `tokeira-iac` describing the convention
    - Add `crates/tokeira-iac/src/resource_modes.rs` as a documentation-only module (no executable code in the public API surface; the module exists solely for its doc comment)
    - Include the convention pattern with the DSQL cluster example from the design document: resource-defined mode enum, persist under `properties["mode"]`, pure `effective_managed` helper, mode-aware `delete()`
    - Reference this module from the `tokeira-iac` crate-root doc comment
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6_

  - [ ] 12.2 Extract `effective_managed` as a pure helper in the DSQL cluster resource
    - In `crates/tokeira-aws/src/resources/dsql_cluster.rs`, define `fn effective_managed(config_mode: DsqlClusterMode, state_mode: &str) -> bool` as a free function with no I/O and no AWS SDK dependencies
    - Rewrite `DsqlCluster::delete` to compute `let effective = effective_managed(self.config.mode, state_mode);` and early-return `Ok(())` when `!effective`
    - Confirm `create()` still persists `"mode": "managed"` or `"mode": "preexisting"` in `ResourceState.properties`
    - _Requirements: 7.4, 7.5_

  - [ ]* 12.3 Write unit tests for `effective_managed` covering all four combinations
    - Assert `effective_managed(Managed, "managed")` is `true`
    - Assert `effective_managed(Managed, "preexisting")` is `true`
    - Assert `effective_managed(Preexisting, "managed")` is `true` (prevents orphaning after config drift)
    - Assert `effective_managed(Preexisting, "preexisting")` is `false`
    - Test location: `crates/tokeira-aws/src/resources/dsql_cluster.rs` `#[cfg(test)]` module
    - _Requirements: 7.8_

- [ ] 13. Final checkpoint — Ensure all tests pass
  - Run `cargo lint` to verify code quality
  - Run `cargo test --workspace` and verify all tests pass, including property tests, unit tests, engine invariants, CAS version tracking, and the `effective_managed` convention tests
  - Run `cargo +nightly fmt --all --check` to verify formatting

## Notes

- All tests are required — property tests are marked `*` per convention but are mandatory per project rules.
- No generic `ResourceMode` enum, no `Resource::mode()` trait method, no `ResourceState.mode` field. Mode-awareness is resource-specific.
- Resources that need lifecycle variation follow the `effective_managed` convention documented in task 12.1. The DSQL cluster resource is the reference implementation; its unit tests cover the four config/state combinations.
- `console` and `indicatif` are added only to the `tkr` binary crate, not to library crates.
- `StateStore::save` changes from returning `Result<(), _>` to `Result<String, _>` so the orchestrator saver can track the latest CAS version across incremental saves. All call sites must be updated.
- The orchestrator removes its final `state_store.save` after `apply` and `destroy`; every save flows through the incremental saver.
- `refresh_state` now saves per pruned resource (once per state change) rather than once per refresh cycle.
- `plan_destroy` is a new engine method added by this spec; the orchestrator's `plan_destroy` facade wires `DestroyMode` around the call.
- Property tests use `proptest` with minimum 100 iterations per the project convention.
- All code must pass `cargo lint` per AGENTS.md.
