# Requirements Document

## Introduction

The `tokeira-iac` crate provides a generic IaC framework with `Resource` and `Module` traits, a plan/apply engine, topological sorting, and state persistence. This spec addresses critical lifecycle management gaps that lead to orphaned resources, stale state, incorrect destroy behavior, and poor operator visibility during long-running operations.

The feature covers seven areas: resource mode persistence, destroy-mode context propagation, incremental crash-safe state saves, module-scoped delete suppression, config writeback after apply, CLI progress reporting with `indicatif`, and describe-before-delete safety during destroy.

## Glossary

- **Engine**: The stateless `tokeira_iac::Engine` struct that coordinates plan/apply/destroy operations over `Resource` objects.
- **ProvisionContext**: The context passed to resource lifecycle methods carrying project identity, tags, state, progress reporters, and typed extensions.
- **ModuleContext**: The context passed to `Module::resources()` for resource assembly, carrying state and typed extensions.
- **InfraComposition**: A composed set of modules carrying `desired_modules`, `known_modules`, and `active_modules`.
- **ResourceState**: The persisted state for a single resource after creation or update, stored in `InfraState.resources`.
- **ResourceMode**: A lifecycle classification for a resource: `Managed` (engine created it), `Preexisting` (engine adopted it but must not delete it), or `Shared` (engine uses it but another system owns deletion).
- **StateSaver**: A callback invoked after each mutating operation so the orchestrator can persist state incrementally.
- **InfraEngine**: The orchestrator facade in `tokeira-orchestrator` that connects the generic engine to a concrete deployment.
- **DestroyMode**: A marker extension registered in `ProvisionContext` during destroy operations, enabling modules to inspect persisted state and decide whether to include a resource in the destroy set.
- **Writeback**: The process of writing infrastructure outputs (physical IDs, endpoints, bucket names) back into the deployment config file after apply.
- **ActionTuiHandle**: A progress reporting handle using `indicatif` that displays spinners, progress bars, and colored output for long-running operations.

## Requirements

### Requirement 1: Resource Mode Persistence

**User Story:** As a platform operator, I want each resource to persist its lifecycle mode (Managed, Preexisting, Shared) in state, so that destroy operations know whether to actually delete a resource and diff operations know the correct comparison strategy.

#### Acceptance Criteria

1. THE Engine SHALL persist a `mode` field in `ResourceState.properties` when creating or updating a resource.
2. WHEN a resource is created by the Engine, THE Engine SHALL record its mode as `Managed` in the persisted state.
3. WHEN a resource is adopted from a preexisting provider object, THE Engine SHALL record its mode as `Preexisting` in the persisted state.
4. WHEN a resource is marked as `Shared`, THE Engine SHALL record its mode as `Shared` in the persisted state.
5. WHEN the Engine encounters a `ResourceState` without a `mode` field during load, THE Engine SHALL default the mode to `Managed` for backward compatibility.
6. FOR ALL valid ResourceMode values, serializing then deserializing SHALL produce an equivalent value (round-trip property).

### Requirement 2: Destroy-Mode Context Extension

**User Story:** As a module author, I want to know when the engine is performing a destroy operation, so that my module can include resources in the destroy set that would otherwise be excluded by current config.

#### Acceptance Criteria

1. WHEN the Engine begins a destroy operation, THE Engine SHALL register a `DestroyMode` marker extension in `ProvisionContext` before calling `Module::resources()`.
2. WHILE `DestroyMode` is registered in `ProvisionContext`, THE ModuleContext SHALL expose the destroy marker to modules via `ModuleContext::extension::<DestroyMode>()`.
3. WHEN `DestroyMode` is present and a resource has mode `Managed` in persisted state, THE Module SHALL include that resource in the known set regardless of current config.
4. WHEN `DestroyMode` is present and a resource has mode `Preexisting` in persisted state, THE Module SHALL NOT include that resource in the destroy set.
5. WHEN `DestroyMode` is absent (normal plan/apply), THE Module SHALL enumerate resources based solely on current config.

### Requirement 3: Incremental State Save (Crash Safety)

**User Story:** As a platform operator, I want state to be persisted after every single create/update/delete operation, so that if the process crashes mid-apply the persisted state accurately reflects what was actually provisioned.

#### Acceptance Criteria

1. WHEN the Engine completes a resource create operation, THE Engine SHALL invoke the `StateSaver` callback before proceeding to the next resource.
2. WHEN the Engine completes a resource update operation, THE Engine SHALL invoke the `StateSaver` callback before proceeding to the next resource.
3. WHEN the Engine completes a resource delete operation, THE Engine SHALL invoke the `StateSaver` callback before proceeding to the next resource.
4. IF the `StateSaver` callback returns an error, THEN THE Engine SHALL abort the apply and return the error to the caller.
5. WHEN the Engine prunes a stale resource from state during refresh, THE Engine SHALL invoke the `StateSaver` callback to persist the pruned state.
6. FOR ALL sequences of N mutating operations, the StateSaver SHALL be invoked exactly N times (one per mutation, idempotence property).

### Requirement 4: Module-Scoped Delete Suppression

**User Story:** As a platform operator, I want `infra apply --module networking` to never delete resources belonging to the `cluster` module, so that module-scoped operations are safe and isolated.

#### Acceptance Criteria

1. WHEN computing changes for a module-scoped operation, THE Engine SHALL suppress Delete changes for resources whose persisted module is not in the active module set.
2. WHEN a resource's persisted module matches one of the active modules, THE Engine SHALL include that resource's Delete change in the filtered plan.
3. THE Engine SHALL preserve all Create and Update changes regardless of module scope.
4. THE Engine SHALL preserve NoChange entries regardless of module scope.
5. FOR ALL module-scoped operations, the set of Delete changes SHALL be a subset of resources owned by active modules (metamorphic property: `deletes ⊆ active_module_resources`).

### Requirement 5: Config Writeback After Apply

**User Story:** As a platform operator, I want infrastructure outputs (endpoints, bucket names, physical IDs) written back to my deployment config file after apply, so that subsequent deployment phases can consume discovered values without manual intervention.

#### Acceptance Criteria

1. WHEN an apply operation completes successfully, THE InfraEngine SHALL collect writeback values from the deployment via `Deployment::collect_writeback()`.
2. WHEN writeback values are non-empty, THE CLI SHALL write those values into the deployment config file using dotted-key TOML insertion.
3. WHEN a writeback key path does not exist in the config file, THE CLI SHALL create intermediate TOML tables as needed.
4. WHEN a writeback key path already exists in the config file, THE CLI SHALL overwrite the existing value with the new value.
5. IF the config file cannot be written, THEN THE CLI SHALL return an error describing the failure.
6. FOR ALL writeback operations with N key-value pairs, the resulting TOML file SHALL contain exactly those N values at their specified paths (round-trip property).

### Requirement 6: CLI Progress Reporting

**User Story:** As a platform operator, I want to see real-time progress during long-running infrastructure operations, so that I know what is happening, what is waiting, and what has completed.

#### Acceptance Criteria

1. WHEN the Engine begins a create operation on a resource, THE CLI SHALL display a spinner with the resource type and ID.
2. WHEN the Engine begins an update operation on a resource, THE CLI SHALL display a spinner with the resource type, ID, and change summary.
3. WHEN the Engine begins a delete operation on a resource, THE CLI SHALL display a spinner with the resource type and ID.
4. WHEN a resource operation completes, THE CLI SHALL replace the spinner with a completion indicator showing elapsed time.
5. WHILE a resource is waiting for a provider condition (polling), THE CLI SHALL display elapsed time and timeout remaining.
6. WHEN the `--json` flag is provided, THE CLI SHALL emit structured JSON progress events instead of terminal UI elements.
7. THE CLI SHALL display a summary line showing total operations completed, failed, and skipped after plan or apply finishes.

### Requirement 7: Describe Before Delete (Destroy Safety)

**User Story:** As a platform operator, I want the engine to verify a resource still exists before attempting to delete it during destroy, so that destroy operations are idempotent and do not fail on already-absent resources.

#### Acceptance Criteria

1. WHEN the Engine is about to delete a resource during destroy, THE Engine SHALL call `describe()` on that resource first.
2. WHEN `describe()` returns `None` (resource absent), THE Engine SHALL prune the resource from state without calling `delete()`.
3. WHEN `describe()` returns `Some(live_state)`, THE Engine SHALL pass the live state to `delete()` instead of the potentially stale persisted state.
4. WHEN `describe()` returns an error, THE Engine SHALL propagate the error and abort the destroy for that resource.
5. FOR ALL destroy operations on N resources where K are already absent, the Engine SHALL call `delete()` exactly N-K times (metamorphic property).

### Requirement 8: ResourceMode in Diff Strategy

**User Story:** As a module author, I want the diff logic to consider resource mode when computing changes, so that Preexisting resources use adoption-aware comparison and Managed resources use full property comparison.

#### Acceptance Criteria

1. WHEN diffing a resource with mode `Managed`, THE Engine SHALL perform a full property comparison between desired and current state.
2. WHEN diffing a resource with mode `Preexisting`, THE Engine SHALL compare only the fields that the engine controls (tags, associations) and ignore provider-managed fields.
3. WHEN diffing a resource with mode `Shared`, THE Engine SHALL report NoChange unless the resource's engine-controlled properties have diverged.
4. THE Resource trait SHALL expose a `mode()` method returning the resource's declared lifecycle mode.
5. FOR ALL resources with mode `Preexisting`, the diff SHALL never produce a Delete change during normal plan/apply (invariant property).
