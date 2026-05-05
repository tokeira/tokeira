# Requirements Document

## Introduction

The `tokeira-iac` crate provides a generic IaC framework with `Resource` and `Module` traits, a plan/apply engine, topological sorting, and state persistence. This spec addresses critical lifecycle management gaps that lead to orphaned resources, stale state, incorrect destroy behavior, and poor operator visibility during long-running operations.

The approach keeps the engine generic. Mode-awareness (e.g., Managed vs Preexisting semantics) lives in each resource implementation that needs lifecycle variation. The engine never encodes policy about what modes exist or how they interact — it just calls `Resource::diff()`, `Resource::delete()`, and records state. This mirrors the proven pattern from prior IaC work where resources like DSQL clusters and shared S3 buckets define their own mode enums, persist mode in `ResourceState.properties`, and implement mode-aware lifecycle methods.

The feature covers six areas:

1. `DestroyMode` marker extension propagated from orchestrator through to modules
2. Incremental crash-safe state saves via the `StateSaver` contract
3. Module-scoped delete suppression
4. Config writeback after apply
5. CLI progress reporting with `indicatif`
6. Describe-before-delete safety during destroy

A seventh area documents the convention for resources with multiple lifecycle modes (reference pattern, not a trait extension).

## Glossary

- **Engine**: The `tokeira_iac::Engine` struct that coordinates plan/apply/destroy operations over `Resource` objects.
- **ProvisionContext**: The context passed to resource lifecycle methods carrying project identity, tags, state, progress reporters, and typed extensions.
- **ModuleContext**: The context passed to `Module::resources()` for resource assembly, carrying state and typed extensions.
- **InfraComposition**: A composed set of modules carrying `desired_modules`, `known_modules`, and `active_modules`.
- **ResourceState**: The persisted state for a single resource after creation or update, stored in `InfraState.resources`.
- **StateSaver**: A callback invoked after each mutating operation so the orchestrator can persist state incrementally.
- **InfraEngine**: The orchestrator facade in `tokeira-orchestrator` that connects the generic engine to a concrete deployment.
- **DestroyMode**: A marker extension registered in `ProvisionContext` during destroy operations, enabling modules to enumerate resources from persisted state in addition to current config.
- **Writeback**: The process of writing infrastructure outputs (physical IDs, endpoints, bucket names) back into the deployment config file after apply.
- **ActionTuiHandle**: A progress reporting handle using `indicatif` that displays spinners, progress bars, and colored output for long-running operations.
- **Resource Mode (convention)**: A resource-specific lifecycle classification (e.g., `Managed`, `Preexisting`) defined by each resource that needs lifecycle variation. Persisted as a string in `ResourceState.properties["mode"]`. Not a trait concept.

## Requirements

### Requirement 1: Destroy-Mode Context Extension

**User Story:** As a module author, I want to know when the engine is performing a destroy operation, so that my module can enumerate resources that should be deleted even when current config would not include them.

#### Acceptance Criteria

1. THE `tokeira-iac` crate SHALL define a `DestroyMode` marker struct in the public API.
2. WHEN the `InfraEngine` facade begins a destroy or `plan_destroy` operation, THE facade SHALL register a `DestroyMode` marker extension in `ProvisionContext` before calling `collect_resources_from`.
3. WHILE `DestroyMode` is registered in `ProvisionContext`, THE `ModuleContext` SHALL expose the marker via `ModuleContext::extension::<DestroyMode>()`.
4. WHEN `DestroyMode` is absent (normal plan/apply), THE `ModuleContext::extension::<DestroyMode>()` SHALL return `None`.
5. THE engine SHALL NOT register `DestroyMode` during plan, apply, or refresh operations.
6. THE `ProvisionContext` SHALL expose a `remove_extension::<T>()` method so the orchestrator can scope the `DestroyMode` registration to a single operation. After a destroy or `plan_destroy` operation returns, the `DestroyMode` extension SHALL be removed before any subsequent operation runs.

### Requirement 2: Incremental State Save (Crash Safety)

**User Story:** As a platform operator, I want state to be persisted after every single create/update/delete operation, so that if the process crashes mid-apply the persisted state accurately reflects what was actually provisioned.

#### Acceptance Criteria

1. WHEN the Engine completes a resource create operation, THE Engine SHALL invoke the `StateSaver` callback before proceeding to the next resource.
2. WHEN the Engine completes a resource update operation, THE Engine SHALL invoke the `StateSaver` callback before proceeding to the next resource.
3. WHEN the Engine completes a resource delete operation, THE Engine SHALL invoke the `StateSaver` callback before proceeding to the next resource.
4. IF the `StateSaver` callback returns an error, THEN THE Engine SHALL abort the current operation and return the error to the caller.
5. WHEN the Engine prunes a stale resource from state during refresh in an apply or destroy operation (a `StateSaver` is available), THE Engine SHALL invoke the `StateSaver` callback once per pruned resource — not once per refresh. WHEN the Engine prunes during a plan operation (no `StateSaver` is available), THE Engine SHALL mutate `ctx.state` in memory only, without attempting to persist.
6. FOR ALL apply and destroy operations with N successful mutating operations (including per-resource refresh prunes), the `StateSaver` SHALL be invoked exactly N times (no batching). Plan operations invoke the `StateSaver` zero times.

### Requirement 2a: CAS Version Tracking Across Incremental Saves

**User Story:** As a platform operator, I want each incremental state save to use the ETag/version returned by the previous save, so that a sequence of saves does not conflict with itself on the S3-backed state store.

#### Acceptance Criteria

1. THE orchestrator's `StateSaver` implementation SHALL track the latest state store version returned by each successful save and use that version for the next save.
2. THE `StateStore::save` method SHALL return the new version (ETag) after a successful write so the saver can update its tracked version.
3. THE orchestrator SHALL NOT perform a final `state_store.save` after `engine.apply` or `engine.destroy` returns — all saves SHALL happen through the incremental saver, using the latest tracked version.
4. IF the `StateSaver` observes a CAS conflict (version mismatch), THEN THE saver SHALL return the error immediately without retry; the engine aborts per Requirement 2.4.
5. THE version tracking SHALL use interior mutability (e.g., `Arc<Mutex<String>>` or `Arc<RwLock<String>>`) because the saver closure is `Fn`, not `FnMut`.

### Requirement 3: Module-Scoped Delete Suppression

**User Story:** As a platform operator, I want `infra apply --module networking` to never delete resources belonging to the `cluster` module, so that module-scoped operations are safe and isolated.

#### Acceptance Criteria

1. WHEN computing changes for a module-scoped operation, THE Engine SHALL suppress Delete changes for resources whose persisted module is not in the active module set.
2. WHEN a resource's persisted module matches one of the active modules, THE Engine SHALL include that resource's Delete change in the filtered plan.
3. THE Engine SHALL preserve all Create changes regardless of module scope.
4. THE Engine SHALL preserve all Update changes regardless of module scope.
5. THE Engine SHALL preserve all NoChange entries regardless of module scope.
6. FOR ALL module-scoped operations, the set of Delete changes SHALL be a subset of resources owned by active modules (`deletes ⊆ active_module_resources`).

### Requirement 4: Config Writeback After Apply

**User Story:** As a platform operator, I want infrastructure outputs (endpoints, bucket names, physical IDs) written back to my deployment config file after apply, so that subsequent deployment phases can consume discovered values without manual intervention.

#### Acceptance Criteria

1. WHEN an apply operation completes successfully, THE InfraEngine SHALL collect writeback values from the deployment via `Deployment::collect_writeback()`.
2. WHEN writeback values are non-empty, THE CLI SHALL write those values into the deployment config file using dotted-key TOML insertion.
3. WHEN a writeback key path does not exist in the config file, THE CLI SHALL create intermediate TOML tables as needed.
4. WHEN a writeback key path already exists in the config file, THE CLI SHALL overwrite the existing value with the new value.
5. THE CLI SHALL preserve existing TOML comments and formatting when writing values back.
6. IF the config file cannot be written, THEN THE CLI SHALL return an error describing the failure.
7. FOR ALL writeback operations with N key-value pairs, reading each value at its specified path after write SHALL produce the original value (round-trip property).

### Requirement 5: CLI Progress Reporting

**User Story:** As a platform operator, I want to see real-time progress during long-running infrastructure operations, so that I know what is happening, what is waiting, and what has completed — including whether each operation succeeded, failed, or was skipped.

#### Acceptance Criteria

1. THE `ProvisionContext` SHALL expose three progress callback registration methods for operation lifecycle: `set_apply_progress` (operation start), `set_complete_progress` (operation completed successfully), and `set_failed_progress` (operation failed with error).
2. THE `ProvisionContext` SHALL retain `set_wait_progress` (periodic polling update) and `set_note_progress` (informational note).
3. THE engine SHALL call `emit_apply_progress` before invoking a resource lifecycle method.
4. THE engine SHALL call `emit_complete_progress` after a resource lifecycle method returns successfully, with elapsed time since the matching `emit_apply_progress`.
5. THE engine SHALL call `emit_failed_progress` when a resource lifecycle method returns an error, with elapsed time and the error message.
6. WHEN the Engine begins a create, update, or delete operation, THE CLI SHALL display a spinner with the resource type and ID.
7. WHEN a resource operation completes successfully, THE CLI SHALL replace the spinner with a completion indicator showing elapsed time.
8. WHEN a resource operation fails, THE CLI SHALL replace the spinner with a failure indicator showing elapsed time and the error.
9. WHILE a resource is waiting for a provider condition (polling), THE CLI SHALL display elapsed time and timeout remaining.
10. WHEN a resource operation emits a note via `emit_note_progress`, THE CLI SHALL display the note associated with the resource.
11. WHEN the `--json` flag is provided, THE CLI SHALL emit structured JSON progress events to stdout (one JSON object per line) instead of terminal UI elements. The JSON schema SHALL include: `OperationStart`, `OperationComplete`, `OperationFailed`, `WaitProgress`, `Note`, and `Summary`.
12. THE CLI SHALL display a summary line showing total operations completed, failed, and skipped after plan, apply, or destroy finishes. THE `completed` and `failed` counts SHALL be derived from the `emit_complete_progress` and `emit_failed_progress` callbacks. THE `skipped` count SHALL be derived from the number of `ChangeKind::NoChange` entries returned in the plan — the engine does not invoke a lifecycle method for a NoChange entry, so no callback fires.
13. WHEN stdout is not a terminal (e.g., piped to a file or log), THE CLI SHALL automatically fall back to plain progress lines instead of ANSI spinners.
14. THE global `--json` flag SHALL be threaded from `Cli::json` through `main` into `commands::infra::run` so that the `infra` command subsystem selects the correct `OutputFormat`.

### Requirement 6: Describe Before Delete (Destroy Safety)

**User Story:** As a platform operator, I want the engine to verify a resource still exists before attempting to delete it during destroy, so that destroy operations are idempotent and do not fail on already-absent resources.

#### Acceptance Criteria

1. WHEN the Engine is about to delete a resource during destroy, THE Engine SHALL call `describe()` on that resource first.
2. WHEN `describe()` returns `None` (resource absent), THE Engine SHALL prune the resource from state without calling `delete()`.
3. WHEN `describe()` returns `Some(live_state)`, THE Engine SHALL pass the live state to `delete()` instead of the potentially stale persisted state.
4. WHEN `describe()` returns an error, THE Engine SHALL propagate the error and abort the destroy for that resource.
5. FOR ALL destroy operations on N resources where K are already absent, the Engine SHALL call `delete()` at most N-K times.

### Requirement 7: Resource Mode Convention (Guideline)

**User Story:** As a resource author, I want a documented convention for implementing resources with multiple lifecycle modes, so that resources like DSQL clusters and shared S3 buckets can express "managed by me" vs "adopted from elsewhere" consistently across the codebase.

This requirement is a guideline, not a trait extension. The engine remains mode-agnostic. Resources that need lifecycle variation follow this convention internally.

#### Acceptance Criteria

1. A resource with multiple lifecycle modes SHALL define its own mode enum (e.g., `DsqlClusterMode` with `Managed` and `Preexisting` variants).
2. The resource SHALL persist its mode as a string under `ResourceState.properties["mode"]` (e.g., `"managed"` or `"preexisting"`).
3. The resource's `diff()` method SHALL inspect its current config mode and, where relevant, the persisted state mode, to decide the diff outcome.
4. The resource SHALL expose a pure `effective_managed(config_mode, state_mode) -> bool` helper function that computes `config_mode == Managed OR state_mode == "managed"`. The helper SHALL be free of I/O and AWS SDK dependencies so it can be unit-tested without provider credentials.
5. The resource's `delete()` method SHALL call the `effective_managed` helper and skip the provider delete call when the helper returns `false`, returning `Ok(())` without side effects.
6. The resource's `create()` method SHALL, when in a non-managed mode, adopt an existing provider object without creating a new one and persist the appropriate mode in `ResourceState.properties["mode"]`.
7. The `effective_managed` predicate ensures a resource originally created as `Managed` but whose config later changed to `Preexisting` is still deleted during destroy, preventing orphaned resources.
8. Unit tests of `effective_managed` SHALL cover all four combinations: `(Managed, "managed")`, `(Managed, "preexisting")`, `(Preexisting, "managed")`, `(Preexisting, "preexisting")`. The only case returning `false` SHALL be `(Preexisting, "preexisting")`.
